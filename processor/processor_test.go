package processor

import (
	"io/ioutil"
	"log"
	"net/http"
	"testing"

	"github.com/go-redis/redis"
	"golang.skroutz.gr/skroutz/downloader/job"
	"golang.skroutz.gr/skroutz/downloader/storage"
)

const storageDir = "./"

var (
	Redis  *redis.Client
	store  *storage.Storage
	logger = log.New(ioutil.Discard, "[test processor]", log.Ldate|log.Ltime)
)

func init() {
	var err error

	Redis = redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	store, err = storage.New(Redis)
	if err != nil {
		log.Fatal(err)
	}
	err = Redis.FlushDB().Err()
	if err != nil {
		log.Fatal(err)
	}
}

func TestRogueCollection(t *testing.T) {
	processor, err := New(store, 3, storageDir, &http.Client{}, logger)
	if err != nil {
		t.Fatal(err)
	}

	testcases := []struct {
		Job           job.Job
		expectedState job.State
	}{
		{
			job.Job{
				ID:            "RogueOne",
				DownloadState: job.StateInProgress,
			},
			job.StatePending,
		},
		{
			job.Job{
				ID:            "Valid",
				DownloadState: job.StateSuccess,
			},
			job.StateSuccess,
		},
	}

	for _, testcase := range testcases {
		store.SaveJob(&testcase.Job)
	}

	processor.collectRogueDownloads()

	for _, testcase := range testcases {
		j, err := store.GetJob(testcase.Job.ID)
		if err != nil {
			t.Fatal(err)
		}

		if j.DownloadState != testcase.expectedState {
			t.Fatalf("Expected job state Pending, found %s", j.DownloadState)
		}
	}

}

//TODO: placeholder func to test perform for all its (edge) cases
func TestPerform(t *testing.T) {
}
