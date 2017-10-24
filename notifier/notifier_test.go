package notifier

import (
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.skroutz.gr/skroutz/downloader/job"
	"golang.skroutz.gr/skroutz/downloader/storage"

	"github.com/go-redis/redis"
)

var (
	Redis    = redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	cbServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	}))
)

func init() {
	err := Redis.FlushDB().Err()
	if err != nil {
		log.Fatal(err)
	}

}

func TestNotifyJobDeletion(t *testing.T) {
	type cases struct {
		j           *job.Job
		shouldExist bool
	}

	testcases := []cases{
		{&job.Job{
			ID:            "successjob",
			URL:           "http://localhost:12345",
			AggrID:        "notifoo",
			DownloadState: job.StateSuccess,
			CallbackURL:   cbServer.URL}, false},

		{&job.Job{
			ID:            "failjob",
			URL:           "http://localhost:12345",
			AggrID:        "notifoo",
			DownloadState: job.StateSuccess,
			CallbackURL:   "http://localhost:39871/nonexistent"}, true},
	}

	st, err := storage.New(Redis)
	if err != nil {
		t.Fatal(err)
	}
	notifier := New(st, 10)

	for _, tc := range testcases {
		err = st.QueuePendingCallback(tc.j)
		if err != nil {
			t.Fatal(err)
		}

		exists, err := st.JobExists(tc.j)
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("Expected job with id %s to exist in Redis", tc.j.ID)
		}

		notifier.Notify(tc.j)

		exists, err = st.JobExists(tc.j)
		if err != nil {
			t.Fatal(err)
		}
		if exists != tc.shouldExist {
			t.Fatalf("Expected job exist to be %v", tc.shouldExist)
		}
	}
}
