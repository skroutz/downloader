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
	fileServer     *http.Server
	fileServerHost = "localhost"
	fileServerPort = "9718"
	fileServerPath = "/testdata/"

	callbackServerHost = "localhost"
	callbackServerPort = "9894"
	callbackServerPath = "/callback/"
	cbChan             = make(chan []byte)
	cbServer           = newCallbackServer(fmt.Sprintf("%s:%s", callbackServerHost, callbackServerPort), cbChan)
)

func TestMain(m *testing.M) {
	// initialize global variables
	copy(originalArgs, os.Args)
	apiClient = http.Client{Timeout: 5 * time.Second}
	mux := http.NewServeMux()
	mux.Handle(fileServerPath, http.StripPrefix(fileServerPath,
		http.FileServer(http.Dir("testdata/"))))
	fileServer = &http.Server{
		Handler: mux,
		Addr:    fmt.Sprintf("%s:%s", fileServerHost, fileServerPort)}

	purgeRedis()

	// start test file server
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := fileServer.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	waitForServer(fileServerPort)

	// start test callback server
	go func() {
		err := cbServer.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	waitForServer(callbackServerPort)

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
	purgeRedis()
	os.Exit(result)
}

func TestResourceExists(t *testing.T) {
	var served, downloaded *os.File
	var expected, actual []byte
	var err error

	// Test job creation (APIServer)
	downloadURL := fmt.Sprintf("http://%s:%s%ssample-1.jpg", fileServerHost, fileServerPort, fileServerPath)
	callbackURL := fmt.Sprintf("http://%s:%s%s", callbackServerHost, callbackServerPort, callbackServerPath)
	jobData := map[string]interface{}{
		"aggr_id":      "asemas",
		"aggr_limit":   1,
		"url":          downloadURL,
		"callback_url": callbackURL}

	postJob(jobData, t)

	// Test job processing (Processor)
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
		case <-time.After(3 * time.Second):
			t.Fatal("File not present on the download location after 5 seconds")
		default:
			filePath := path.Join(cfg.Processor.StorageDir, "999")
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

	// Test callback mechanism (Notifier)
	var parsedCB notifier.CallbackInfo

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Callback request receive timeout")
	case cb := <-cbChan:
		err = json.Unmarshal(cb, &parsedCB)
		if err != nil {
			t.Fatalf("Error parsing callback response: %s | %s", err, string(cb))
		}
		if parsedCB.Success != true {
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
}

func TestResourceDontExist(t *testing.T) {
	var parsedCB notifier.CallbackInfo

	downloadURL := fmt.Sprintf("http://%s:%s%si-dont-exist.foo", fileServerHost, fileServerPort, fileServerPath)
	callbackURL := fmt.Sprintf("http://%s:%s%s", callbackServerHost, callbackServerPort, callbackServerPath)
	jobData := map[string]interface{}{
		"aggr_id":      "foobar",
		"aggr_limit":   1,
		"url":          downloadURL,
		"callback_url": callbackURL}

	postJob(jobData, t)

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Callback request receive timeout")
	case cb := <-cbChan:
		err := json.Unmarshal(cb, &parsedCB)
		if err != nil {
			t.Fatalf("Error parsing callback response: %s | %s", err, string(cb))
		}
		// Success:false, Error:"", Extra:"", DownloadURL:"http://localhost/wCNMXVXROtz2EQ"}
		if parsedCB.Success != false {
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
	mux.HandleFunc(callbackServerPath, handler)

	return &http.Server{
		Handler:           mux,
		Addr:              addr,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		ReadHeaderTimeout: 3 * time.Second,
	}
}

// TODO: should read addr from config
func purgeRedis() {
	err := redisClient("test", "localhost:6379").FlushDB().Err()
	if err != nil {
		log.Fatal(err)
	}
}