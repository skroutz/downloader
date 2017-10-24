package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

	"golang.skroutz.gr/skroutz/downloader/notifier"
)

type testJob map[string]interface{}

var (
	mu           sync.Mutex
	originalArgs = make([]string, len(os.Args))

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
	fsChan     = make(chan int, 10)
	fileServer = newFileServer(fmt.Sprintf("%s:%s", fsHost, fsPort), fsChan)

	csHost = "localhost"
	csPort = "9894"
	csPath = "/callback/"
	// callbacks from cbServer are emitted to this channel
	callbacks = make(chan []byte, 1000)
	cbServer  = newCallbackServer(fmt.Sprintf("%s:%s", csHost, csPort), callbacks)
)

func TestMain(m *testing.M) {
	// initialize global variables
	copy(originalArgs, os.Args)

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
	go func() {
		err := cbServer.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	waitForServer(csPort)

	// start API server, Processor & Notifier
	componentsWg.Add(1)
	go start("api", "--host", apiHost, "--port", apiPort)
	waitForServer(apiPort)

	componentsWg.Add(1)
	go start("processor")
	// circumvent race conditions with os.Args
	time.Sleep(500 * time.Millisecond)

	componentsWg.Add(1)
	go start("notifier")

	result := m.Run()

	// shutdown test file server
	log.Println("Shutting down test file server...")
	err := fileServer.Shutdown(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	// shutdown test callback server
	log.Println("Shutting down test callback server...")
	err = cbServer.Shutdown(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Shutting down API, Processor & Notifier..")
	sigCh <- os.Interrupt // shutdown APIServer
	sigCh <- os.Interrupt // shutdown Processor
	sigCh <- os.Interrupt // shutdown Notifier

	componentsWg.Wait()
	flushRedis()
	os.Exit(result)
}

func TestResourceExists(t *testing.T) {
	var served, downloaded *os.File
	var expected, actual []byte
	var err error

	// Test job creation (APIServer)
	jobData := testJob{
		"aggr_id":      "asemas",
		"aggr_limit":   1,
		"url":          downloadURL("sample-1.jpg"),
		"callback_url": callbackURL(),
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
		if !strings.HasPrefix(ci.DownloadURL, "http://localhost/") {
			t.Fatalf("Expected DownloadURL to begin with 'http://localhost/': %#v",
				ci)
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
			filePath := path.Join(cfg.Processor.StorageDir, downloadURI.Path)
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

	job := testJob{
		"aggr_id":      "foobar",
		"aggr_limit":   1,
		"url":          downloadURL("i-dont-exist.foo"),
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
		if ci.Extra != "" {
			t.Fatalf("Expected Extra to be empty: %#v", ci)
		}
	}
}

// test a download URL that will fail the first 2 times but succeeds the 3rd
func TestTransientDownstreamError(t *testing.T) {
	var ci notifier.CallbackInfo

	job := testJob{
		"aggr_id":      "murphy",
		"aggr_limit":   1,
		"url":          fmt.Sprintf("http://%s:%s%ssample-1.jpg", fsHost, fsPort, fsPath),
		"callback_url": fmt.Sprintf("http://%s:%s%s", csHost, csPort, csPath)}

	fsChan <- http.StatusInternalServerError // 1st try
	fsChan <- http.StatusServiceUnavailable  // 2nd try
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
		if !strings.HasPrefix(ci.DownloadURL, "http://localhost/") {
			t.Fatalf("Expected DownloadURL to begin with 'http://localhost/': %#v",
				ci)
		}
	}
}

func TestLoad(t *testing.T) {
	var wg sync.WaitGroup
	nreqs := 300
	aggrs := []string{"loadtest0", "loadtest1", "loadtest2", "loadtest3", "loadtest4"}
	rand.Seed(time.Now().Unix())

	genJob := func(url string) testJob {
		return testJob{
			"aggr_id":      aggrs[rand.Intn(len(aggrs))],
			"aggr_limit":   nreqs / 2,
			"url":          downloadURL(url),
			"callback_url": callbackURL(),
			"extra":        "foo"}
	}

	if nreqs%2 != 0 {
		t.Fatalf("nreqs must be an even number, was %d", nreqs)
	}

	// success jobs
	for i := 0; i < nreqs/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := postJob(genJob("load-test.jpg"))
			if err != nil {
				log.Fatal(err)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			err := postJob(genJob("i-dont-exist.foo"))
			if err != nil {
				log.Fatal(err)
			}
		}()
	}

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

	if results[true] != nreqs/2 {
		t.Fatalf("Expected %d successful downloads, got %d", nreqs/2, results[true])
	}

	if results[false] != nreqs/2 {
		t.Fatalf("Expected %d failed downloads, got %d", nreqs/2, results[false])
	}

	wg.Wait()
}

// executes main() with the provided args.
func start(args ...string) {
	// TODO: maybe locking is not needed
	// especially if we start the processes sequentially
	mu.Lock()
	os.Args = originalArgs
	os.Args = append(os.Args, args...)
	mu.Unlock()

	main()
	componentsWg.Done()
}

// blocks until the API server is up
func waitForServer(port string) {
	time.Sleep(500 * time.Millisecond)
	conn, err := net.DialTimeout("tcp", "localhost:"+port, timeout)
	if err != nil {
		log.Fatal(err)
	}
	conn.Close()
}

// Creates a Job by executing a request to the API server
func postJob(job testJob) error {
	// TODO: get download endpoint from config
	uri := fmt.Sprintf("http://%s:%s/download", apiHost, apiPort)

	v, _ := json.Marshal(job)
	resp, err := apiClient.Post(uri, "application/json", bytes.NewBuffer(v))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		info, _ := httputil.DumpResponse(resp, true)
		log.Println(string(info))
		return errors.New(fmt.Sprintf("Expected 201 response, got %d", resp.StatusCode))
	}
	return nil
}

func newFileServer(addr string, ch chan int) *http.Server {
	mux := http.NewServeMux()

	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextRespCode := -1

		select {
		case nextRespCode = <-ch:
		default:
		}

		if nextRespCode == -1 {
			http.FileServer(http.Dir("testdata/")).ServeHTTP(w, r)
		} else {
			w.WriteHeader(nextRespCode)
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
