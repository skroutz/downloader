package processor

import (
	"context"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"golang.skroutz.gr/skroutz/downloader/job"
	"golang.skroutz.gr/skroutz/downloader/processor/mimetype"
)

var (
	mux         = http.NewServeMux()
	server      = httptest.NewServer(mux)
	defaultAggr = job.Aggregation{ID: "FooBar", Limit: 1}
	defaultWP   workerPool
)

func getTestJob(t *testing.T) job.Job {
	return job.Job{
		ID:          t.Name(),
		URL:         strings.Join([]string{server.URL, t.Name()}, "/"),
		AggrID:      defaultAggr.ID,
		CallbackURL: "http://example.com",
	}
}

func TestMain(m *testing.M) {
	DefaultProcessor, err := New(store, 3, storageDir, logger)
	if err != nil {
		log.Fatal(err)
	}

	defaultWP = DefaultProcessor.newWorkerPool(defaultAggr)

	exit := m.Run()

	server.Close()
	os.Exit(exit)
}

func addHandler(endpoint string, handler func(w http.ResponseWriter, r *http.Request)) {
	if !strings.HasPrefix(endpoint, "/") {
		endpoint = "/" + endpoint
	}
	mux.HandleFunc(endpoint, handler)
}

func TestPerformUserAgent(t *testing.T) {
	var wg sync.WaitGroup

	//Since we are messing with the default settings, we create a new processor here
	p, err := New(store, 1, storageDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	p.UserAgent = "Downloader Test"
	wp := p.newWorkerPool(defaultAggr)

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
	j := getTestJob(t)
	//Use a known external url instead of messing with local certs
	j.URL = "https://untrusted-root.badssl.com/"

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
		t.Fatal("Could not create a new validator")
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
