package processor

import (
	"context"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/skroutz/downloader/job"
	"github.com/skroutz/downloader/processor/mimetype"
)

func TestPerformUserAgent(t *testing.T) {
	var wg sync.WaitGroup

	//Since we are messing with the default settings, we create a new processor here
	p, err := New(store, 1, storageDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	p.UserAgent = "Downloader Test"
	wp, err := p.newWorkerPool(*defaultAggr)
	if err != nil {
		log.Fatal(err)
	}

	j := getTestJob(t)
	var ua = make(chan string)

	addHandler(t.Name(), func(w http.ResponseWriter, r *http.Request) {
		_, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Fatal(err)
		}
		ua <- r.Header.Get("User-Agent")
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		wp.download(context.TODO(), &j, nil)
	}()

	actual := <-ua
	if actual != p.UserAgent {
		t.Fatalf("Expected User-Agent to be %s, got %s", p.UserAgent, actual)
	}
	wg.Wait()
}

func TestProxy(t *testing.T) {
	var wg sync.WaitGroup
	//Since we are messing with the default settings, we create a new processor here
	p, err := New(store, 1, storageDir, logger)
	if err != nil {
		t.Fatal(err)
	}

	a, err := job.NewAggregation("proxyfoo", 2, "http://www.example.com")
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
