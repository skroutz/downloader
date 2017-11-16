package processor

import (
	"context"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"golang.skroutz.gr/skroutz/downloader/job"
	"golang.skroutz.gr/skroutz/downloader/storage"
)

var (
	storageDir string
	Redis      *redis.Client
	store      *storage.Storage
	logger     = log.New(os.Stderr, "[test processor]", log.Ldate|log.Ltime|log.Lshortfile)
)

func init() {
	var err error

	storageDir, err = ioutil.TempDir("", "downloader-processor-")
	if err != nil {
		log.Fatal(err)
	}

	Redis = redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	err = Redis.FlushDB().Err()
	if err != nil {
		log.Fatal(err)
	}

	store, err = storage.New(Redis)
	if err != nil {
		log.Fatal(err)
	}

	//defer os.RemoveAll(dir)
}

func TestReaper(t *testing.T) {
	err := Redis.FlushDB().Err()
	if err != nil {
		t.Fatal(err)
	}

	processor, err := New(store, 3, storageDir, &http.Client{}, logger)
	if err != nil {
		t.Fatal(err)
	}

	err = os.Mkdir(path.Join(storageDir, "RIP"), os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.TODO())
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		processor.reaper(ctx)
		wg.Done()
	}()

	cases := []struct {
		Job     job.Job
		InRedis bool
	}{
		{
			job.Job{ID: "RIPinRedis"},
			true,
		},
		{
			job.Job{ID: "RIPGhost"},
			false,
		},
	}

	for _, tc := range cases {
		if tc.InRedis {
			err := store.SaveJob(&tc.Job)
			if err != nil {
				t.Fatal(err)
			}
		}

		_, err = os.Create(path.Join(storageDir, tc.Job.Path()))
		if err != nil {
			t.Fatal(err)
		}

		err = store.QueueJobForDeletion(tc.Job.ID)
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(2 * time.Second)
	cancel()
	wg.Wait()

	for _, tc := range cases {
		exists, err := store.JobExists(&tc.Job)
		if err != nil {
			t.Fatal(err)
		}

		if exists {
			t.Fatal("Expected Job not to exist in Redis")
		}

		if _, err := os.Stat(path.Join(storageDir, tc.Job.Path())); !os.IsNotExist(err) {
			t.Fatal("Expected file not to exist")
		}
	}
}

func TestRogueCollection(t *testing.T) {
	err := Redis.FlushDB().Err()
	if err != nil {
		t.Fatal(err)
	}

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
				AggrID:        "foobar",
				DownloadState: job.StateInProgress,
			},
			job.StatePending,
		},
		{
			job.Job{
				ID:            "Valid",
				AggrID:        "foobar",
				DownloadState: job.StateSuccess,
			},
			job.StateSuccess,
		},
	}

	for _, testcase := range testcases {
		err = store.SaveJob(&testcase.Job)
		if err != nil {
			t.Fatal(err)
		}
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
func TestPerformUserAgent(t *testing.T) {
	var wg sync.WaitGroup
	ua := make(chan string)

	err := Redis.FlushDB().Err()
	if err != nil {
		t.Fatal(err)
	}

	aggr, err := job.NewAggregation("baz", 1)
	if err != nil {
		t.Fatal(err)
	}
	err = store.SaveAggregation(aggr)
	if err != nil {
		t.Fatal(err)
	}

	j := &job.Job{
		ID:          "jdsk231",
		URL:         "http://localhost:8543/foo.jpg",
		AggrID:      aggr.ID,
		CallbackURL: "http://example.com",
	}

	handler := func(w http.ResponseWriter, r *http.Request) {
		_, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Fatal(err)
		}
		ua <- r.Header.Get("User-Agent")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/foo.jpg", handler)
	server := http.Server{Handler: mux, Addr: ":8543"}

	go func() {
		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	waitForServer("8543")

	err = store.QueuePendingDownload(j)
	if err != nil {
		t.Fatal(err)
	}

	processor, err := New(store, 3, storageDir, &http.Client{}, logger)
	if err != nil {
		t.Fatal(err)
	}
	processor.UserAgent = "Downloader Test"

	wg.Add(1)
	go func() {
		defer wg.Done()
		wp := processor.newWorkerPool(*aggr)
		wp.perform(context.TODO(), j)
	}()

	actual := <-ua
	if actual != processor.UserAgent {
		t.Fatalf("Expected User-Agent to be %s, got %s", processor.UserAgent, actual)
	}
	wg.Wait()
}

// blocks until a server listens on the given port
func waitForServer(port string) {
	backoff := 50 * time.Millisecond

	for i := 0; i < 10; i++ {
		conn, err := net.DialTimeout("tcp", ":"+port, 3*time.Second)
		if err != nil {
			time.Sleep(backoff)
			continue
		}
		conn.Close()
		return
	}
	log.Fatalf("Server on port %s not up after 10 retries", port)
}
