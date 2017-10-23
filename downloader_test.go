package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
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

var (
	mu           sync.Mutex
	originalArgs = make([]string, len(os.Args))

	wg sync.WaitGroup

	apiClient http.Client
	apiHost   = "localhost"
	apiPort   = "8123"

	// An HTTP file server that serves the contents of testdata/.
	// Used to test Processor.
	fsHost     = "localhost"
	fsPort     = "9718"
	fsPath     = "/testdata/"
	fsChan     = make(chan int, 10)
	fileServer = newFileServer(fmt.Sprintf("%s:%s", fsHost, fsPort), fsChan)

	csHost   = "localhost"
	csPort   = "9894"
	csPath   = "/callback/"
	csChan   = make(chan []byte)
	cbServer = newCallbackServer(fmt.Sprintf("%s:%s", csHost, csPort), csChan)

	timeout = 5 * time.Second
)

func TestMain(m *testing.M) {
	// initialize global variables
	copy(originalArgs, os.Args)
	apiClient = http.Client{Timeout: timeout}

	flushRedis()

	// start test file server
	wg.Add(1)
	go func() {
		defer wg.Done()
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
	wg.Add(1)
	go start("api", "--host", apiHost, "--port", apiPort)
	waitForServer(apiPort)

	wg.Add(1)
	go start("processor")
	// circumvent race conditions with os.Args
	time.Sleep(500 * time.Millisecond)

	wg.Add(1)
	go start("notifier")

	result := m.Run()

	// shutdown test file server
	err := fileServer.Shutdown(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	// shutdown test callback server
	err = cbServer.Shutdown(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	sigCh <- os.Interrupt // shutdown APIServer
	sigCh <- os.Interrupt // shutdown Processor
	sigCh <- os.Interrupt // shutdown Notifier

	wg.Wait()
	//purgeRedis()
	os.Exit(result)
}

func TestResourceExists(t *testing.T) {
	var served, downloaded *os.File
	var expected, actual []byte
	var err error

	// Test job creation (APIServer)
	downloadURL := fmt.Sprintf("http://%s:%s%ssample-1.jpg", fsHost, fsPort, fsPath)
	callbackURL := fmt.Sprintf("http://%s:%s%s", csHost, csPort, csPath)
	jobData := map[string]interface{}{
		"aggr_id":      "asemas",
		"aggr_limit":   1,
		"url":          downloadURL,
		"callback_url": callbackURL}

	postJob(jobData, t)

	// Test callback mechanism (Notifier)
	var parsedCB notifier.CallbackInfo

	select {
	case <-time.After(timeout):
		t.Fatal("Callback request receive timeout")
	case cb := <-csChan:
		err = json.Unmarshal(cb, &parsedCB)
		if err != nil {
			t.Fatalf("Error parsing callback response: %s | %s", err, string(cb))
		}
		if !parsedCB.Success {
			t.Fatalf("Expected Success to be true: %#v", parsedCB)
		}
		if parsedCB.Error != "" {
			t.Fatalf("Expected Error to be empty: %#v", parsedCB)
		}
		if parsedCB.Extra != "" {
			t.Fatalf("Expected Extra to be empty: %#v", parsedCB)
		}
		if !strings.HasPrefix(parsedCB.DownloadURL, "http://localhost/") {
			t.Fatalf("Expected DownloadURL to begin with 'http://localhost/': %#v",
				parsedCB)
		}
	}

	// Test job processing (Processor)
	downloadURI, err := url.Parse(parsedCB.DownloadURL)
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
	var parsedCB notifier.CallbackInfo

	job := map[string]interface{}{
		"aggr_id":      "foobar",
		"aggr_limit":   1,
		"url":          fmt.Sprintf("http://%s:%s%si-dont-exist.foo", fsHost, fsPort, fsPath),
		"callback_url": fmt.Sprintf("http://%s:%s%s", csHost, csPort, csPath)}

	postJob(job, t)

	select {
	case <-time.After(timeout):
		t.Fatal("Callback request receive timeout")
	case cb := <-csChan:
		err := json.Unmarshal(cb, &parsedCB)
		if err != nil {
			t.Fatalf("Error parsing callback response: %s | %s", err, string(cb))
		}
		if parsedCB.Success {
			t.Fatal("Expected Success to be false")
		}
		if !strings.HasSuffix(parsedCB.Error, "404") {
			t.Fatalf("Expected Error to end with '404': %s", parsedCB.Error)
		}
		if parsedCB.Extra != "" {
			t.Fatalf("Expected Extra to be empty: %#v", parsedCB)
		}
		if !strings.HasPrefix(parsedCB.DownloadURL, "http://localhost/") {
			t.Fatalf("Expected DownloadURL to begin with 'http://localhost/': %#v",
				parsedCB)
		}
	}
}

// test a download URL that will fail the first 2 times but succeeds the 3rd
func TestTransientDownstreamError(t *testing.T) {
	var parsedCB notifier.CallbackInfo

	job := map[string]interface{}{
		"aggr_id":      "murphy",
		"aggr_limit":   1,
		"url":          fmt.Sprintf("http://%s:%s%ssample-1.jpg", fsHost, fsPort, fsPath),
		"callback_url": fmt.Sprintf("http://%s:%s%s", csHost, csPort, csPath)}

	fsChan <- http.StatusInternalServerError // 1st try
	fsChan <- http.StatusServiceUnavailable  // 2nd try
	// 3rd try will succeed

	postJob(job, t)

	select {
	// bump timeout cause we also have to wait for the backoffs
	case <-time.After(timeout + (5 * time.Second)):
		t.Fatal("Callback request receive timeout")
	case cb := <-csChan:
		err := json.Unmarshal(cb, &parsedCB)
		if err != nil {
			t.Fatalf("Error parsing callback response: %s | %s", err, string(cb))
		}
		if !parsedCB.Success {
			t.Fatal("Expected Success to be true")
		}
		if parsedCB.Error != "" {
			t.Fatalf("Expected Error to be empty", parsedCB.Error)
		}
		if parsedCB.Extra != "" {
			t.Fatalf("Expected Extra to be empty: %#v", parsedCB)
		}
		if !strings.HasPrefix(parsedCB.DownloadURL, "http://localhost/") {
			t.Fatalf("Expected DownloadURL to begin with 'http://localhost/': %#v",
				parsedCB)
		}
	}
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
	wg.Done()
}

// blocks until the API server is up
func waitForServer(port string) {
	time.Sleep(500 * time.Millisecond)
	conn, err := net.DialTimeout("tcp", "localhost:"+port, 3*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	conn.Close()
}

// Creates a Job by executing a request to the API server
func postJob(data map[string]interface{}, t *testing.T) {
	// TODO: get download endpoint from config
	uri := fmt.Sprintf("http://%s:%s/download", apiHost, apiPort)

	v, _ := json.Marshal(data)
	resp, err := apiClient.Post(uri, "application/json", bytes.NewBuffer(v))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		info, _ := httputil.DumpResponse(resp, true)
		log.Println(string(info))
		t.Fatalf("Expected 201 response, got %d", resp.StatusCode)
	}
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
	return &http.Server{Handler: mux, Addr: addr}
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
