package processor

import (
	"context"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/skroutz/downloader/job"
	"github.com/skroutz/downloader/processor/mimetype"
)

func TestPerformWithDefaultRequestHeaders(t *testing.T) {
	processor, err := New(store, 1, storageDir, logger, fileStorage)
	if err != nil {
		t.Fatal(err)
	}

	// Emulate default values as if they would be read
	// from a configuration file
	processor.RequestHeaders = map[string]string{
		"User-Agent":      "Downloader-Agent",
		"Accept":          "*/*",
		"Accept-Encoding": "gzip,deflate,br",
	}

	aggregation, err := job.NewAggregation("FooBarBaz", 1, "", "")
	if err != nil {
		log.Fatal(err)
	}
	wp, err := processor.newWorkerPool(*aggregation)
	if err != nil {
		t.Fatal(err)
	}

	job := getTestJob(t)
	job.RequestHeaders = map[string]string{}
	job.AggrID = aggregation.ID

	var rh = make(chan map[string]string)

	addHandler(t.Name(), func(w http.ResponseWriter, r *http.Request) {
		_, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Fatal(err)
		}

		sniffedHeaders := make(map[string]string)
		for k, values := range r.Header {
			// Loop over all values for the key.
			// We only have one value. This is a loop
			// because the internal implementation of
			// Header associates a key with a list of values.
			for _, value := range values {
				sniffedHeaders[k] = value
			}
		}

		rh <- sniffedHeaders
	})

	waitGroup := new(sync.WaitGroup)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		wp.download(context.TODO(), &job, nil)
	}()

	actualHeaders := <-rh
	// Ensure our goroutine exits before the end
	// because in case of fatalf the goroutine might stay hanged
	waitGroup.Wait()

	if !reflect.DeepEqual(actualHeaders, processor.RequestHeaders) {
		t.Fatalf("Expected request headers to be %#v instead got %#v",
			processor.RequestHeaders, actualHeaders)
	}
}

func TestPerformWithJobRequestHeaders(t *testing.T) {
	processor, err := New(store, 1, storageDir, logger, fileStorage)
	if err != nil {
		t.Fatal(err)
	}

	processor.RequestHeaders = map[string]string{
		"User-Agent": "Downloader-Agent",
		"Accept":     "*/*",
	}

	aggregation, err := job.NewAggregation("FooBarBaz", 1, "", "")
	if err != nil {
		log.Fatal(err)
	}
	wp, err := processor.newWorkerPool(*aggregation)
	if err != nil {
		t.Fatal(err)
	}

	expectedHeaders := map[string]string{
		"Accept":          "application/html,application/xhtml;q=0.8,*/*",
		"Accept-Encoding": "gzip,deflate,br",
		"User-Agent":      "DL-Agent v1.0",
	}

	job := getTestJob(t)
	job.RequestHeaders = expectedHeaders
	job.AggrID = aggregation.ID

	var rh = make(chan map[string]string)

	addHandler(t.Name(), func(w http.ResponseWriter, r *http.Request) {
		_, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Fatal(err)
		}

		sniffedHeaders := make(map[string]string)
		for k, values := range r.Header {
			for _, value := range values {
				sniffedHeaders[k] = value
			}
		}

		rh <- sniffedHeaders
	})

	waitGroup := new(sync.WaitGroup)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		wp.download(context.TODO(), &job, nil)
	}()

	actualHeaders := <-rh

	waitGroup.Wait()

	if !reflect.DeepEqual(actualHeaders, expectedHeaders) {
		t.Fatalf("Expected request headers to be %#v instead got %#v",
			expectedHeaders, actualHeaders)
	}
}

func TestProxy(t *testing.T) {
	var wg sync.WaitGroup
	//Since we are messing with the default settings, we create a new processor here
	p, err := New(store, 1, storageDir, logger, fileStorage)
	if err != nil {
		t.Fatal(err)
	}

	a, err := job.NewAggregation("proxyfoo", 2, "http://www.example.com", "")
	if err != nil {
		t.Fatal(err)
	}
	store.SaveAggregation(a)

	wp, err := p.newWorkerPool(*a)
	if err != nil {
		t.Fatalf("Failed to initialize WorkerPool %s", err)
	}

	// used to check Proxy for the http Client
	req, err := http.NewRequest("GET", "www.google.com", nil)
	if err != nil {
		t.Fatal("Failed to initialize request")
	}
	trans, ok := wp.client.Transport.(*http.Transport)
	if !ok {
		log.Fatal("Expected to be Transport")
	}
	proxyURL, err := trans.Proxy(req)
	if err != nil {
		log.Fatal("Expected to fetch a valid proxy", err)
	}
	actual := proxyURL.String()

	if actual != a.Proxy {
		t.Fatalf("Expected Proxy to be %s, got %s", a.Proxy, actual)
	}
	wg.Wait()
}

func TestValidatorPanic(t *testing.T) {
	// Recover from a generated panic
	defer func() {
		if r := recover(); r != nil {
			log.Println("Recovered panic:", r)
		}
	}()

	j := getTestJob(t)
	j.MimeType = "image/png"

	addHandler(t.Name(), func(w http.ResponseWriter, r *http.Request) {
	})

	// Call to download without a validator should panic
	defaultWP.download(context.TODO(), &j, nil)

	t.Fatal("Should not have reached this. Download should have paniced without a mime type validator")
}

func TestInsecureDownload(t *testing.T) {
	//instantiate simple insecure TLS server
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	}))
	defer ts.Close()

	j := getTestJob(t)
	j.URL = ts.URL

	e := defaultWP.download(context.TODO(), &j, nil)
	if e.IsRetriable() || e.IsInternal() {
		t.Fatal("TLS Errors should not be retriable nor internal", e)
	}
}

func TestResourceNotFound(t *testing.T) {
	addHandler(t.Name(), func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Resource Not Found", http.StatusNotFound)
	})

	j := getTestJob(t)
	e := defaultWP.download(context.TODO(), &j, nil)
	if e.IsRetriable() || e.IsInternal() {
		t.Fatal("Download Error should not be retriable nor internal", e)
	}
}

func TestInternalServerError(t *testing.T) {
	addHandler("/"+t.Name(), func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	})

	j := getTestJob(t)

	e := defaultWP.download(context.TODO(), &j, nil)
	if !e.IsRetriable() || e.IsInternal() {
		t.Fatal("Download Error should be retriable ", e)
	}
}

func TestMimeTypeError(t *testing.T) {
	addHandler(t.Name(), func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "../testdata/tiny.png")
	})

	j := getTestJob(t)
	j.MimeType = "image/jpeg"

	v, err := mimetype.New()
	if err != nil {
		t.Fatal("Could not create a new validator", err)
	}
	e := defaultWP.download(context.TODO(), &j, v)
	if e.IsRetriable() || e.IsInternal() {
		t.Fatal("MimeType Mismatch should not be retriable nor internal", e)
	}

	if !strings.Contains(e.Error(), "mime-type") {
		t.Fatal("Error should contain mime-type mismatch indication")
	}
}

func TestContextCanceling(t *testing.T) {
	addHandler(t.Name(), func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "../testdata/tiny.png")
	})

	j := getTestJob(t)
	c, cancel := context.WithCancel(context.Background())

	cancel()
	e := defaultWP.download(c, &j, nil)

	if !strings.Contains(e.Error(), context.Canceled.Error()) {
		t.Fatal("Download should have returned a context canceled error", e.Err())
	}

	if !e.IsRetriable() || e.IsInternal() {
		t.Fatal("Context cancelled errors should be retriable", e)
	}
}

func TestPerformDownloadSuccess(t *testing.T) {
	var err error
	j := getTestJob(t)
	store.QueuePendingDownload(&j, 0)
	addHandler(t.Name(), func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "../testdata/tiny.png")
	})
	defaultWP.perform(context.TODO(), &j, nil)

	j, err = store.GetJob(j.ID)
	if err != nil {
		t.Fatal(err)
	}

	if j.DownloadState != job.StateSuccess {
		t.Fatalf("Download should have been marked successful for job %s", j)
	}
}

func TestPerformDownloadFail(t *testing.T) {
	var err error
	j := getTestJob(t)
	store.QueuePendingDownload(&j, 0)
	addHandler(t.Name(), func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "NotFound", http.StatusNotFound)
	})
	defaultWP.perform(context.TODO(), &j, nil)

	j, err = store.GetJob(j.ID)

	if err != nil {
		t.Fatal(err)
	}

	if j.DownloadState != job.StateFailed {
		t.Fatalf("Download should have been marked as Failed for job %s", j)
	}
}

func TestPerformDownloadRequeue(t *testing.T) {
	var err error
	j := getTestJob(t)
	store.QueuePendingDownload(&j, 0)
	addHandler(t.Name(), func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	})
	defaultWP.perform(context.TODO(), &j, nil)

	j, err = store.GetJob(j.ID)

	if err != nil {
		t.Fatal(err)
	}

	if j.DownloadState != job.StatePending {
		t.Fatalf("Download should have been Requeued for job %s", j)
	}

	if j.DownloadCount != 1 {
		t.Fatalf("Download count should have been bumped, found DownloadCount: %d", j.DownloadCount)
	}
}
