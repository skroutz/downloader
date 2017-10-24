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
	store *storage.Storage
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

	notifier := New(store, 10)

	for _, tc := range testcases {
		err := store.QueuePendingCallback(tc.j)
		if err != nil {
			t.Fatal(err)
		}

		exists, err := store.JobExists(tc.j)
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("Expected job with id %s to exist in Redis", tc.j.ID)
		}

		notifier.Notify(tc.j)

		exists, err = store.JobExists(tc.j)
		if err != nil {
			t.Fatal(err)
		}
		if exists != tc.shouldExist {
			t.Fatalf("Expected job exist to be %v", tc.shouldExist)
		}
	}
}

func TestRogueCollection(t *testing.T) {
	notifier := New(store, 10)

	testcases := []struct {
		Job           job.Job
		expectedState job.State
	}{
		{
			job.Job{
				ID:            "RogueOne",
				CallbackState: job.StateInProgress,
			},
			job.StatePending,
		},
		{
			job.Job{
				ID:            "Valid",
				CallbackState: job.StateFailed,
			},
			job.StateFailed,
		},
	}

	for _, tc := range testcases {
		store.SaveJob(&tc.Job)
	}

	//start and close Notifier
	ch := make(chan struct{})
	go notifier.Start(ch)
	ch <- struct{}{}
	<-ch

	for _, tc := range testcases {
		j, err := store.GetJob(tc.Job.ID)
		if err != nil {
			t.Fatal(err)
		}

		if j.CallbackState != tc.expectedState {
			t.Fatalf("Expected job state %s, found %s", tc.expectedState, j.DownloadState)
		}
	}
}
