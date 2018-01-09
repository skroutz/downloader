package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.skroutz.gr/skroutz/downloader/notifier"
)

type testJob map[string]interface{}

type fsResponse struct {
	Code    int
	Headers map[string]string
}

var (
	mu         sync.Mutex
	testBinary = os.Args[0:1]

	componentsWg sync.WaitGroup

	// global test limit used in HTTP & disk I/O
	timeout = 10 * time.Second

	apiClient = &http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
		Timeout:   timeout}
	apiHost = "localhost"
	apiPort = "8123"

	// An HTTP file server that serves the contents of testdata/.
	// Used to test Processor.
	fsHost = "localhost"
	fsPort = "9718"
	fsPath = "/testdata/"
	// used to instrument fileServer
	fsChan     = make(chan fsResponse, 10)
	fileServer = newFileServer(fmt.Sprintf("%s:%s", fsHost, fsPort), fsChan)

	csHost = "localhost"
	csPort = "9894"
	csPath = "/callback/"
	// callbacks from cbServer are emitted to this channel
	callbacks = make(chan []byte, 1000)
	cbServer  = newCallbackServer(fmt.Sprintf("%s:%s", csHost, csPort), callbacks)
)

var testConfig string

func TestMain(m *testing.M) {
	// Hijack TestMain & spawn downloader, see spawn()
	if os.Getenv("SPAWN_DOWNLOADER") != "" {
		fmt.Printf("TestMain(spawn): %+v\n", os.Args)
		main()
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stop
		cancel()
	}()

	flag.StringVar(&testConfig, "config", "config.test.json", "Test config")
	flag.Parse()
	parseConfig(testConfig)

	flushRedis()

	// start test file server
	componentsWg.Add(1)
	go func() {
		defer componentsWg.Done()
		err := fileServer.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	waitForServer(fsPort)

	// start test callback server
	componentsWg.Add(1)
	go func() {
		defer componentsWg.Done()
		err := cbServer.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	waitForServer(csPort)

	// start API server, Processor & Notifier
	start := func(args ...string) {
		defer componentsWg.Done()
		spawn(ctx, args...)
	}

	componentsWg.Add(1)
	go start("api", "--host", apiHost, "--port", apiPort, "--config", testConfig)
	waitForServer(apiPort)

	componentsWg.Add(1)
	go start("processor", "--config", testConfig)

	componentsWg.Add(1)
	go start("notifier", "--config", testConfig)

	result := m.Run()

	// shutdown test file server
	log.Println("Shutting down test file server...")
	err := fileServer.Shutdown(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// shutdown test callback server
	log.Println("Shutting down test callback server...")
	err = cbServer.Shutdown(ctx)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Shutting down API, Processor & Notifier..")
	stop <- os.Interrupt

	componentsWg.Wait()
	flushRedis()
	os.Exit(result)
}

func TestResourceExists(t *testing.T) {
	var served, downloaded *os.File
	var expected, actual []byte
	var err error

	resourceURL := downloadURL("sample-1.jpg")

	// Test job creation (APIServer)
	jobData := testJob{
		"aggr_id":      "aggrFOO",
		"aggr_limit":   1,
		"url":          resourceURL,
		"callback_url": callbackURL(),
		"mime_type":    "!image/vnd.adobe.photoshop,image/jpeg",
		"extra":        "foobar"}

	err = postJob(jobData)
	if err != nil {
		t.Fatal(err)
	}

	// Test callback mechanism (Notifier)
	var ci notifier.CallbackInfo

	select {
	case <-time.After(timeout):
		t.Fatal("Callback request receive timeout")
	case cb := <-callbacks:
		err = json.Unmarshal(cb, &ci)
		if err != nil {
			t.Fatalf("Error parsing callback response: %s | %s", err, string(cb))
		}
		if !ci.Success {
			t.Fatalf("Expected Success to be true: %#v", ci)
		}
		if ci.Error != "" {
			t.Fatalf("Expected Error to be empty: %#v", ci)
		}
		if ci.Extra != "foobar" {
			t.Fatalf("Expected Extra to be 'foobar', was %s", ci.Extra)
		}
		if ci.ResourceURL != resourceURL {
			t.Fatalf("Expected ResourceURL to be %s, was %s", resourceURL, ci.ResourceURL)
		}
		if !strings.HasPrefix(ci.DownloadURL, "http://localhost/") {
			t.Fatalf("Expected DownloadURL to begin with 'http://localhost/': %#v",
				ci)
		}
		if ci.JobID == "" {
			t.Fatalf("Expected JobID to be set: %#v", ci)
		}
		if ci.ResponseCode != 200 {
			t.Fatalf("Expected ResponseCode to be set: %#v", ci)
		}
	}

	// Test job processing (Processor)
	downloadURI, err := url.Parse(ci.DownloadURL)
	if err != nil {
		t.Fatal(err)
	}
	served, err = os.Open("testdata/sample-1.jpg")
	if err != nil {
		t.Fatal(err)
	}
	servedFileStat, err := served.Stat()
	if err != nil {
		t.Fatal(err)
	}

FILECHECK:
	for {
		select {
		case <-time.After(timeout):
			t.Fatal("File not present on the download location after 5 seconds")
		default:
			relativePath := strings.TrimPrefix(downloadURI.String(), cfg.Notifier.DownloadURL)
			filePath := path.Join(cfg.Processor.StorageDir, relativePath)
			downloaded, err = os.Open(filePath)
			if err == nil {
				downloadedFileStat, err := downloaded.Stat()
				if err != nil {
					log.Fatal(err)
				}

				if downloadedFileStat.Size() == servedFileStat.Size() {
					break FILECHECK
				}
			} else {
				fmt.Printf("Expected file not found (%s), retrying (%s)...\n",
					filePath, err)
				time.Sleep(50 * time.Millisecond)
			}
		}
	}

	expected, err = ioutil.ReadAll(served)
	if err != nil {
		t.Fatal(err)
	}

	actual, err = ioutil.ReadAll(downloaded)
	if err != nil {
		t.Fatal(err)
	}

	if len(actual) <= 0 {
		t.Fatal("Downloaded file was empty")
	}

	if !reflect.DeepEqual(actual, expected) {
		t.Error("Expected downloaded and served files to be equal")
	}
}

func TestResourceDontExist(t *testing.T) {
	var ci notifier.CallbackInfo

	resourceURL := downloadURL("i-dont-exist.foo")
	job := testJob{
		"aggr_id":      "foobar",
		"aggr_limit":   1,
		"url":          resourceURL,
		"callback_url": callbackURL()}

	err := postJob(job)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-time.After(timeout):
		t.Fatal("Callback request receive timeout")
	case cb := <-callbacks:
		err := json.Unmarshal(cb, &ci)
		if err != nil {
			t.Fatalf("Error parsing callback response: %s | %s", err, string(cb))
		}
		if ci.Success {
			t.Fatal("Expected Success to be false")
		}
		if !strings.HasSuffix(ci.Error, "404") {
			t.Fatalf("Expected Error to end with '404': %s", ci.Error)
		}
		if ci.DownloadURL != "" {
			t.Fatalf("Expected DownloadURL to be empty: %#v", ci)
		}
		if ci.ResourceURL != resourceURL {
			t.Fatalf("Expected ResourceURL to be %s, was %s", resourceURL, ci.ResourceURL)
		}
		if ci.Extra != "" {
			t.Fatalf("Expected Extra to be empty: %#v", ci)
		}
		if ci.JobID == "" {
			t.Fatalf("Expected JobID to be set: %#v", ci)
		}
		if ci.ResponseCode != 404 {
			t.Fatalf("Expected ResponseCode to be set: %#v", ci)
		}
	}
}

func TestMimeTypeMismatch(t *testing.T) {
	var ci notifier.CallbackInfo

	resourceURL := downloadURL("tiny.png")
	job := testJob{
		"aggr_id":      "foobar",
		"aggr_limit":   1,
		"url":          resourceURL,
		"callback_url": callbackURL(),
		"mime_type":    "image/jpeg",
	}

	err := postJob(job)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-time.After(timeout):
		t.Fatal("Callback request receive timeout")
	case cb := <-callbacks:
		err := json.Unmarshal(cb, &ci)
		if err != nil {
			t.Fatalf("Error parsing callback response: %s | %s", err, string(cb))
		}
		if ci.Success {
			t.Fatal("Expected Success to be false")
		}
		if !strings.HasPrefix(ci.Error, "Expected mime-type") {
			t.Fatalf("Expected Error to start with Expected mime-type': %s", ci.Error)
		}
	}
}

// test a download URL that will fail the first 2 times but succeeds the 3rd
func TestTransientDownstreamError(t *testing.T) {
	var ci notifier.CallbackInfo

	resourceURL := fmt.Sprintf("http://%s:%s%ssample-1.jpg", fsHost, fsPort, fsPath)

	job := testJob{
		"aggr_id":      "murphy",
		"aggr_limit":   1,
		"url":          resourceURL,
		"callback_url": fmt.Sprintf("http://%s:%s%s", csHost, csPort, csPath)}

	fsChan <- fsResponse{http.StatusInternalServerError, nil} // 1st try
	fsChan <- fsResponse{http.StatusServiceUnavailable, nil}  // 2nd try
	// 3rd try will succeed

	err := postJob(job)
	if err != nil {
		t.Fatal(err)
	}

	select {
	// bump timeout cause we also have to wait for the backoffs
	case <-time.After(timeout + (5 * time.Second)):
		t.Fatal("Callback request receive timeout")
	case cb := <-callbacks:
		err := json.Unmarshal(cb, &ci)
		if err != nil {
			t.Fatalf("Error parsing callback response: %s | %s", err, string(cb))
		}
		if !ci.Success {
			t.Fatal("Expected Success to be true")
		}
		if ci.Error != "" {
			t.Fatalf("Expected Error to be empty, was '%s'", ci.Error)
		}
		if ci.Extra != "" {
			t.Fatalf("Expected Extra to be empty: %#v", ci)
		}
		if ci.ResourceURL != resourceURL {
			t.Fatalf("Expected ResourceURL to be %s, was %s", resourceURL, ci.ResourceURL)
		}
		if !strings.HasPrefix(ci.DownloadURL, "http://localhost/") {
			t.Fatalf("Expected DownloadURL to begin with 'http://localhost/': %#v",
				ci)
		}
		if ci.JobID == "" {
			t.Fatalf("Expected JobID to be set: %#v", ci)
		}
		if ci.ResponseCode != 200 {
			t.Fatalf("Expected ResponseCode to be set: %#v", ci)
		}
	}
}

// TODO: this should be extracted to package-specific integration tests
// this is not the place to test such functionality
func TestStats(t *testing.T) {
	var res *http.Response
	var err error
	stats := make(map[string]int)

	jobs := []testJob{
		{"aggr_id": "statsfoo",
			"aggr_limit":   4,
			"url":          downloadURL("sample-1.jpg"),
			"callback_url": callbackURL(),
			"extra":        "foobar"},
		{"aggr_id": "statsbar",
			"aggr_limit":   4,
			"url":          downloadURL("sample-1.jpg"),
			"callback_url": callbackURL(),
			"extra":        "foobar"},
	}

	var wg sync.WaitGroup

	for _, tj := range jobs {
		wg.Add(1)
		go func(j testJob) {
			defer wg.Done()
			err := postJob(j)
			if err != nil {
				log.Fatal(err)
			}
		}(tj)
	}

	<-callbacks
	<-callbacks

	found := false
	for i := 0; i < 5; i++ {
		time.Sleep(500 * time.Millisecond)
		res, err = apiClient.Get(fmt.Sprintf("http://%s:%s/stats/processor", apiHost, apiPort))
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != 200 {
			continue
		}
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		err = json.Unmarshal(body, &stats)
		if err != nil {
			t.Fatal(err)
		}
		if stats["spawnedWorkerPools"] < 2 {
			continue
		}
		found = true
		break
	}
	defer res.Body.Close()

	if !found {
		t.Fatal("Not found processor stats after 5 tries:", stats)
	}

	if stats["spawnedWorkerPools"] < 2 {
		t.Fatal("Expected spawnedWorkerPools to be less than or equal to 2:", stats)
	}
	if stats["maxWorkerPools"] > stats["spawnedWorkerPools"] {
		t.Fatal("Expected maxWorkerPools to be <= spawnedWorkerPools", stats)
	}
	if stats["workerPools"] > stats["spawnedWorkerPools"] {
		t.Fatal("Expected workerPools to be <= spawnedWorkerPools", stats)
	}

	// workers
	if stats["spawnedWorkers"] < 2 {
		t.Fatal("Expected spawnedWorkers to be larger than 2:", stats)
	}
	if stats["maxWorkers"] > stats["spawnedWorkers"] {
		t.Fatal("Expected maxWorkers to be <= spawnedWorkers", stats)
	}
	if stats["workers"] > stats["spawnedWorkers"] {
		t.Fatal("Expected workers to be <= spawnedWorkers", stats)
	}

	wg.Wait()
}

type jobAttrs struct {
	url  string
	mime string
}

func TestLoad(t *testing.T) {
	nreqs := 300
	naggrs := 5
	limit := 10

	var wg sync.WaitGroup

	aggrs := make([]string, naggrs)

	for n := 0; n < naggrs; n++ {
		aggrs[n] = fmt.Sprintf("loadtest:%d", n)
	}

	rand.Seed(time.Now().Unix())

	genJob := func(attrs jobAttrs) testJob {
		return testJob{
			"aggr_id":      aggrs[rand.Intn(len(aggrs))],
			"aggr_limit":   limit,
			"url":          downloadURL(attrs.url),
			"callback_url": callbackURL(),
			"mime_type":    attrs.mime,
			"extra":        "foo"}
	}

	if nreqs%2 != 0 {
		t.Fatalf("nreqs must be an even number, was %d", nreqs)
	}

	attrs := make(chan jobAttrs, nreqs)

	// API client pool
	for i := 0; i < 60; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attr := range attrs {
				err := postJob(genJob(attr))
				if err != nil {
					log.Fatal(err)
				}
			}
		}()
	}

	for i := 0; i < nreqs/3; i++ {
		attrs <- jobAttrs{"load-test.jpg", "image/jpeg"}
		attrs <- jobAttrs{"load-test.jpg", "image/png"}
		attrs <- jobAttrs{"i-dont-exist.foo", ""}
	}
	close(attrs)

	var ci notifier.CallbackInfo
	results := make(map[bool]int, nreqs)

	for i := 0; i < nreqs; i++ {
		select {
		case <-time.After(timeout):
			t.Fatal("Callback request receive timeout")
		case cb := <-callbacks:
			err := json.Unmarshal(cb, &ci)
			if err != nil {
				t.Fatalf("Could not parse callback data: %s (%s)", err, cb)
			}
			if ci.Extra != "foo" {
				t.Fatalf("Expected Extra to be 'foo', was '%s'", ci.Extra)
			}
			if ci.Success {
				if !strings.HasPrefix(ci.DownloadURL, "http://localhost/") {
					t.Fatalf("Expected DownloadURL to begin with 'http://localhost/', was '%s'",
						ci.DownloadURL)
				}
			} else {
				if ci.DownloadURL != "" {
					t.Fatalf("Expected DownloadURL to be empty, was '%s'", ci.DownloadURL)
				}
			}
			results[ci.Success]++
		}
	}

	if results[true] != nreqs/3 {
		t.Fatalf("Expected %d successful downloads, got %d", nreqs/2, results[true])
	}

	if results[false] != 2*nreqs/3 {
		t.Fatalf("Expected %d failed downloads, got %d", nreqs/2, results[false])
	}

	wg.Wait()
}

// spawn a downloader subprocess with the provided args.
//
// We do that by fork-executing the test binary with the special SPAWN_DOWNLOADER environment
// variable set. TestMain() checks for SPAWN_DOWNLOADER and executes downloader's main().
func spawn(ctx context.Context, args ...string) {
	var cmdWg sync.WaitGroup

	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = []string{"SPAWN_DOWNLOADER=1", "DOWNLOADER_TEST_TIME=1"}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		log.Fatalf("spawn(%s): %v", args[0], err)
	}

	cmdWg.Add(1)
	go func() {
		defer cmdWg.Done()
		if err := cmd.Wait(); err != nil {
			log.Fatalf("spawn(%s): %v", args[0], err)
		}
	}()

	<-ctx.Done()
	cmd.Process.Signal(syscall.SIGTERM)

	cmdWg.Wait()
}

// blocks until a server listens on the given port
func waitForServer(port string) {
	backoff := 50 * time.Millisecond

	for i := 0; i < 10; i++ {
		conn, err := net.DialTimeout("tcp", ":"+port, timeout)
		if err != nil {
			time.Sleep(backoff)
			continue
		}
		conn.Close()
		return
	}
	log.Fatalf("Server on port %s not up after 10 retries", port)
}

// Creates a Job by executing a request to the API server
func postJob(job testJob) error {
	// TODO: get download endpoint from config
	uri := fmt.Sprintf("http://%s:%s/download", apiHost, apiPort)

	v, _ := json.Marshal(job)

	resp, err := apiClient.Post(uri, "application/json", bytes.NewBuffer(v))
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		info, _ := httputil.DumpResponse(resp, true)
		log.Println(string(info))
		return fmt.Errorf("Expected 201 response, got %d", resp.StatusCode)
	}
	return nil
}

func newFileServer(addr string, ch chan fsResponse) *http.Server {
	mux := http.NewServeMux()

	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case nextResponse := <-ch:
			for k, v := range nextResponse.Headers {
				w.Header().Set(k, v)
			}
			w.WriteHeader(nextResponse.Code)
		default:
			http.FileServer(http.Dir("testdata/")).ServeHTTP(w, r)
		}
	})

	mux.Handle(fsPath, http.StripPrefix(fsPath, h))
	return &http.Server{
		Handler:           mux,
		Addr:              addr,
		ReadTimeout:       timeout,
		WriteTimeout:      timeout,
		ReadHeaderTimeout: timeout,
	}
}

// a test callback server used to test the Notifier. When it receives a request,
// it emits it back to ch.
func newCallbackServer(addr string, ch chan []byte) *http.Server {
	handler := func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Fatal(err)
		}
		ch <- body
	}

	mux := http.NewServeMux()
	mux.HandleFunc(csPath, handler)

	return &http.Server{
		Handler:           mux,
		Addr:              addr,
		ReadTimeout:       timeout,
		WriteTimeout:      timeout,
		ReadHeaderTimeout: timeout,
	}
}

// TODO: should read addr from config
func flushRedis() {
	err := redisClient("test", "localhost:6379").FlushDB().Err()
	if err != nil {
		log.Fatal(err)
	}
}

func downloadURL(filename string) string {
	return fmt.Sprintf("http://%s:%s%s%s", fsHost, fsPort, fsPath, filename)
}

func callbackURL() string {
	return fmt.Sprintf("http://%s:%s%s", csHost, csPort, csPath)
}
