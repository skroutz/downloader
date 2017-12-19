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

	"github.com/go-redis/redis"
	"golang.skroutz.gr/skroutz/downloader/job"
	"golang.skroutz.gr/skroutz/downloader/storage"
)

var (
	Redis  = redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	store  *storage.Storage
	logger = log.New(os.Stderr, "[test-api] ", log.Ldate|log.Ltime)
)

func init() {
	err := Redis.FlushDB().Err()
	if err != nil {
		log.Fatal(err)
	}
	store, err = storage.New(Redis)
	if err != nil {
		log.Fatal(err)
	}
}

func TestHandler(t *testing.T) {
	cases := map[string]int{
		`{"aggr_id":"foo","aggr_limit":8,"url":"https://httpbin.org/image/png","callback_url":"http://localhost:8080", "extra":"foobar"}`: http.StatusCreated,

		`meh`:           http.StatusBadRequest,
		`{"goo":"bar"}`: http.StatusBadRequest,
		// invalid aggregation (no limit)
		`{"aggr_id":"foo","url":"https://httpbin.org/image/png","callback_url":"http://localhost:8080", "extra":"foobar"}`: http.StatusBadRequest,
		// invalid job (no callback url)
		`{"aggr_id":"foo","aggr_limit":8,"url":"https://httpbin.org/image/png","extra":"foobar"}`: http.StatusBadRequest,
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
