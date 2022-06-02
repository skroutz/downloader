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
	"path"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agis/spawn"

	"github.com/skroutz/downloader/config"
	"github.com/skroutz/downloader/job"
)

type testJob map[string]interface{}

type fsResponse struct {
	Code    int
	Headers map[string]string
}

var (
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
	callbacks chan []byte
	cbServer  *http.Server
)

var testConfig string

func TestMain(m *testing.M) {
	flag.StringVar(&testConfig, "config", "config.test.json", "Test config")
	flag.Parse()
	var err error
	cfg, err = config.Parse(testConfig)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// start API server, Processor & Notifier
	api := spawn.New(main, "api", "--host", apiHost, "--port", apiPort, "--config", testConfig)
	err = api.Start(ctx)
	if err != nil {
		panic(err)
	}
	waitForServer(apiPort)

	processor := spawn.New(main, "processor", "--config", testConfig)
	processor.Cmd.Env = append(processor.Cmd.Env, "DOWNLOADER_TEST_TIME=1")
	err = processor.Start(ctx)
	if err != nil {
		panic(err)
	}

	notifier := spawn.New(main, "notifier", "--config", testConfig)
	notifier.Cmd.Env = append(notifier.Cmd.Env, "DOWNLOADER_TEST_TIME=1")
	err = notifier.Start(ctx)
	if err != nil {
		panic(err)
	}

	flushRedis(cfg.Redis.Addr)

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

	callbacks = make(chan []byte, 1000)
	cbServer = newCallbackServer(fmt.Sprintf("%s:%s", csHost, csPort), callbacks)

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

	result := m.Run()

	// shutdown test file server
	err = fileServer.Shutdown(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// shutdown test callback server
	err = cbServer.Shutdown(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// signal API, Processor & Notifier to shutdown
	cancel()

	// wait for test fileserver and callback server to shutdown
	componentsWg.Wait()

	flushRedis(cfg.Redis.Addr)

	err = api.Wait()
	if err != nil {
		log.Fatal(err)
	}

	err = processor.Wait()
	if err != nil {
		log.Fatal(err)
	}

	err = notifier.Wait()
	if err != nil {
		log.Fatal(err)
	}

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
	var ci job.Callback

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
		if ci.ImageSize != "" {
			t.Fatalf("Expected to not trigger image-size extraction, got: '%s'", ci.ImageSize)
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
			relativePath := strings.TrimPrefix(downloadURI.String(), cfg.Processor.DownloadURL)
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
	var ci job.Callback

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
		if !strings.Contains(ci.Error, "404 Not Found") {
			t.Fatalf("Expected Error to contain '404 Not Found': %s", ci.Error)
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
	var ci job.Callback

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
		if !strings.Contains(ci.Error, "Expected mime-type") {
			t.Fatalf("Expected Error to contain Expected mime-type': %s", ci.Error)
		}
	}
}

func TestExtractImageSize(t *testing.T) {
	type test struct {
		file_in             string
		mime_type_in        string
		expected_image_size string
	}

	tests := []test{
		{"tiny.png", "", "10x10"},
		{"tiny.pdf", "", ""},
		{"tiny.bmp", "", ""},
		{"tiny.pdf", "application/pdf", ""},
	}

	var ci job.Callback
	for _, tc := range tests {
		job := testJob{
			"aggr_id":            "foobar",
			"aggr_limit":         1,
			"url":                downloadURL(tc.file_in),
			"callback_url":       callbackURL(),
			"mime_type":          tc.mime_type_in,
			"extract_image_size": true,
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

			if !ci.Success {
				t.Fatal("Expected Success to be true")
			}

			if len(tc.expected_image_size) == 0 && ci.ImageSize != "" {
				t.Fatalf("Expected no ImageSize, got: '%s'", ci.ImageSize)
			}

			if len(tc.expected_image_size) > 0 && !strings.Contains(ci.ImageSize, tc.expected_image_size) {
				t.Fatalf("Expected an ImageSize of '%s', got: '%s'", tc.expected_image_size, ci.ImageSize)
			}
		}
	}
}

// test a download URL that will fail the first 2 times but succeeds the 3rd
func TestTransientDownstreamError(t *testing.T) {
	var ci job.Callback

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

func TestNoRetries(t *testing.T) {
	var ci job.Callback

	resourceURL := downloadURL("tiny.png")

	job := testJob{
		"aggr_id":      "no-retries",
		"aggr_limit":   1,
		"url":          resourceURL,
		"callback_url": fmt.Sprintf("http://%s:%s%s", csHost, csPort, csPath),
		"max_retries":  0}

	fsChan <- fsResponse{http.StatusInternalServerError, nil} // 1st try
	// there shoundn't be a 2nd try (we want the processor to stop after the first error)

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
		if ci.Success {
			t.Fatal("Expected Success to be false")
		}
		if ci.Error == "" {
			t.Fatal("Expected Error not to be empty")
		}
		if ci.ResponseCode != http.StatusInternalServerError {
			t.Fatal("Expected an Interal Server Error")
		}
	}
}

func TestOneRetry(t *testing.T) {
	var ci job.Callback

	resourceURL := downloadURL("tiny.png")

	job := testJob{
		"aggr_id":      "one-retry",
		"aggr_limit":   1,
		"url":          resourceURL,
		"callback_url": fmt.Sprintf("http://%s:%s%s", csHost, csPort, csPath),
		"max_retries":  1}

	fsChan <- fsResponse{http.StatusInternalServerError, nil} // 1st try
	fsChan <- fsResponse{http.StatusInternalServerError, nil} // 2nd try
	// there shoundn't be a 2nd try (we want the processor to stop after the second error)

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
		if ci.Success {
			t.Fatal("Expected Success to be false")
		}
		if ci.Error == "" {
			t.Fatal("Expected Error not to be empty")
		}
		if ci.ResponseCode != http.StatusInternalServerError {
			t.Fatal("Expected an Interal Server Error")
		}
	}
}

func TestUnexpectedReadError(t *testing.T) {
	var ci job.Callback

	resourceURL := downloadURL("200")

	job := testJob{
		"aggr_id":      "murphy",
		"aggr_limit":   1,
		"url":          resourceURL,
		"callback_url": callbackURL(),
		"mime_type":    "type/missmatch",
	}
	headers := make(map[string]string)
	headers["Content-Length"] = "42"             // larger that the actual reply
	fsChan <- fsResponse{http.StatusOK, headers} // 1st  try
	fsChan <- fsResponse{http.StatusOK, headers} // 2nd  try
	fsChan <- fsResponse{http.StatusOK, headers} // last try

	err := postJob(job)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-time.After(timeout + (5 * time.Second)):
		t.Fatal("Callback request receive timeout")
	case cb := <-callbacks:
		err := json.Unmarshal(cb, &ci)
		if err != nil {
			t.Fatalf("Error parsing callback response: %s | %s", err, string(cb))
		}
		if ci.Success {
			t.Fatal("Expected download to be unsuccesful")
		}
		if !strings.Contains(ci.Error, "unexpected EOF") {
			t.Fatalf("Expected to get an 'unexcepted EOF' error, got %q", ci.Error)
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

	var ci job.Callback
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

func flushRedis(address string) {
	err := redisClient("test", address).FlushDB().Err()
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
