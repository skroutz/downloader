package api

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	klog "github.com/go-kit/kit/log"
	"github.com/go-redis/redis"
	"github.com/skroutz/downloader/config"
	"github.com/skroutz/downloader/job"
	"github.com/skroutz/downloader/storage"
)

var (
	Redis   *redis.Client
	store   *storage.Storage
	logger  klog.Logger
	testCfg = "../config.test.json"
)

func init() {
	cfg, err := config.Parse(testCfg)
	if err != nil {
		log.Fatal(err)
	}
	Redis = redis.NewClient(&redis.Options{Addr: cfg.Redis.Addr})
	err = Redis.FlushDB().Err()
	if err != nil {
		log.Fatal(err)
	}
	store, err = storage.New(Redis)
	if err != nil {
		log.Fatal(err)
	}

	logger = klog.NewLogfmtLogger(klog.NewSyncWriter(os.Stderr))
	logger = klog.With(logger, "component", "api")
}

func TestHandler(t *testing.T) {
	cases := map[string]int{
		`{"aggr_id":"foo","aggr_limit":8,"url":"https://httpbin.org/image/png","callback_url":"http://localhost:8080", "extra":"foobar"}`:                http.StatusCreated,
		`{"aggr_id":"foo","aggr_limit":8,"url":"https://httpbin.org/image/png","callback_url":"http://localhost:8080", "extra":"{\"info\":\"foobar\"}"}`: http.StatusCreated,
		`{"aggr_id":"foo","aggr_limit":8,"url":"https://httpbin.org/image/png","callback_url":"http://localhost:8080"}`:                                  http.StatusCreated,
		`meh`:           http.StatusBadRequest,
		`{"goo":"bar"}`: http.StatusBadRequest,
		// invalid aggregation (no limit)
		`{"aggr_id":"foo","url":"https://httpbin.org/image/png","callback_url":"http://localhost:8080", "extra":"foobar"}`: http.StatusBadRequest,
		// invalid job (no callback url)
		`{"aggr_id":"foo","aggr_limit":8,"url":"https://httpbin.org/image/png","extra":"foobar"}`: http.StatusBadRequest,
		// invalid extra (JSON is invalid)
		`{"aggr_id":"foo","aggr_limit":8,"url":"https://httpbin.org/image/png","callback_url":"http://localhost:8080", "extra":"{"info":"foobar"}"}`: http.StatusBadRequest,
	}

	as := New(store, "example.com", 80, "", logger)

	for data, expected := range cases {
		req := httptest.NewRequest("POST", "/download", strings.NewReader(data))
		w := httptest.NewRecorder()
		as.ServeHTTP(w, req)

		result := w.Result()
		body, err := ioutil.ReadAll(result.Body)
		if err != nil {
			t.Fatal(err)
		}

		if result.StatusCode != expected {
			t.Fatalf("Expected status code %d, got %d (%s)", expected, result.StatusCode, data)
		}

		if result.StatusCode == http.StatusCreated {
			v := make(map[string]string)
			err := json.Unmarshal(body, &v)
			if err != nil {
				t.Fatal(err)
			}
			if !(len(v["id"]) > 0) {
				t.Fatalf("Expected to receive a valid job id, got %s", body)
			}
		}
	}
}

func TestRetryHandler(t *testing.T) {
	testcases := map[string]int{
		`AqUCDp0PUWAKAw`: http.StatusNoContent,
		`PendingState`:   http.StatusBadRequest,
		`NonExisting`:    http.StatusBadRequest,
		``:               http.StatusBadRequest,
	}

	testJob1 := job.Job{ID: "AqUCDp0PUWAKAw", CallbackState: job.StateFailed}
	testJob2 := job.Job{ID: "PendingState", CallbackState: job.StatePending}
	as := New(store, "example.com", 80, "", logger)
	err := as.Storage.SaveJob(&testJob1)
	if err != nil {
		t.Fatal(err)
	}
	err = as.Storage.SaveJob(&testJob2)
	if err != nil {
		t.Fatal(err)
	}

	for id, expected := range testcases {
		req := httptest.NewRequest("POST", "/retry/"+id, nil)
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(as.retry)
		handler.ServeHTTP(rr, req)
		result := rr.Result()

		if result.StatusCode != expected {
			t.Errorf("Expected status code %d, got %d (%s)", expected, result.StatusCode, id)
		}
	}
}
