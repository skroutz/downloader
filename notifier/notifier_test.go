package notifier

import (
	"encoding/json"
	"expvar"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/skroutz/downloader/config"
	"github.com/skroutz/downloader/job"
	"github.com/skroutz/downloader/stats"
	"github.com/skroutz/downloader/storage"

	"github.com/go-redis/redis"
)

var (
	Redis    *redis.Client
	cbServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	store    *storage.Storage
	logger   = log.New(os.Stderr, "[test-notifier] ", log.Ldate|log.Ltime)
	testCfg  = "../config.test.json"
	Backends map[string]map[string]interface{}
)

func init() {
	cfg, err := config.Parse(testCfg)
	if err != nil {
		log.Fatal(err)
	}

	Backends = cfg.Backends

	Redis = redis.NewClient(&redis.Options{Addr: cfg.Redis.Addr})
	err = Redis.FlushDB().Err()
	if err != nil {
		log.Fatal(err)
	}
	store, err = storage.New(Redis)
	if err != nil {
		log.Fatal(err)
	}
}

func TestUndefinedBackend(t *testing.T) {
	j := &job.Job{
		ID:            "successjob",
		URL:           "http://localhost:12345",
		AggrID:        "notifoo",
		DownloadState: job.StateSuccess,
		CallbackType:  "foo",
		CallbackDst:   cbServer.URL}

	notifier, err := New(store, 10, logger, "http://blah.com/")
	if err != nil {
		t.Fatal(err)
	}

	notifier.StatsIntvl = 50 * time.Millisecond

	notifier.stats = stats.New(statsID, notifier.StatsIntvl, func(m *expvar.Map) {
		// Store metrics in JSON
		err := notifier.Storage.SetStats("notifier", m.String(), 2*notifier.StatsIntvl)
		if err != nil {
			notifier.Log.Println("Could not report stats", err)
		}
	})

	ch := make(chan struct{})
	go notifier.Start(ch, Backends)

	err = store.QueuePendingCallback(j, 0)
	if err != nil {
		t.Fatal(err)
	}

	var dataBytes []byte
	var data string

	type result struct {
		Alive                        int `json:"alive"`
		UndefinedBackendWithCallback int `json:"undefinedBackendWithCallback"`
	}

	for i := 0; i < 5; i++ {
		time.Sleep(50 * time.Millisecond)

		dataBytes, err = store.GetStats("notifier")
		if err != nil {
			t.Fatalf("Error returned while accessing stats %s", err)
		}

		data = string(dataBytes)

		if data != "" {
			var r result
			err = json.Unmarshal(dataBytes, &r)
			if err != nil {
				t.Fatalf("Error while unmarsalling to results struct. Error is %s", err)
			}

			if r.UndefinedBackendWithCallback == 1 {
				break
			} else {
				t.Fatalf("Expected %d Got %d for undefinedBackendWithCallback", 1, r.UndefinedBackendWithCallback)
			}
		}
	}

	if data == "" {
		t.Fatalf("Did not receive stats data for 250 millis")
	}

	ch <- struct{}{}
	<-ch
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

	statsID = "jobdeletion"
	notifier, err := New(store, 10, logger, "http://blah.com/")
	if err != nil {
		t.Fatal(err)
	}

	ch := make(chan struct{})
	go notifier.Start(ch, Backends)

	for _, tc := range testcases {
		err := store.QueuePendingCallback(tc.j, 0)
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

		time.Sleep(2 * time.Second)

		exists, err = store.JobExists(tc.j)
		if err != nil {
			t.Fatal(err)
		}
		if exists != tc.shouldExist {
			t.Fatalf("Expected job exist to be %v", tc.shouldExist)
		}
	}

	ch <- struct{}{}
	<-ch
}

func TestRogueCollection(t *testing.T) {
	statsID = "rogue"
	notifier, err := New(store, 10, logger, "http://blah.com/")
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
		err := store.SaveJob(&tc.Job)
		if err != nil {
			t.Fatal(err)
		}
	}

	//start and close Notifier
	ch := make(chan struct{})
	go notifier.Start(ch, Backends)
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
