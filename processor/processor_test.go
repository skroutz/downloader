package processor

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/skroutz/downloader/config"
	"github.com/skroutz/downloader/job"
	"github.com/skroutz/downloader/processor/diskcheck"
	"github.com/skroutz/downloader/storage"
)

var (
	storageDir       string
	Redis            *redis.Client
	store            *storage.Storage
	testChecker      *dummyDiskChecker
	logger           = log.New(os.Stderr, "[test processor]", log.Ldate|log.Ltime|log.Lshortfile)
	mux              = http.NewServeMux()
	server           = httptest.NewServer(mux)
	defaultAggr      = job.Aggregation{ID: "FooBar", Limit: 1}
	defaultProcessor Processor
	defaultWP        workerPool
	testCfg          = "../config.test.json"
)

func TestMain(m *testing.M) {
	var err error

	storageDir, err = ioutil.TempDir("", "downr-processor-")
	if err != nil {
		log.Fatal(err)
	}

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

	// Bypass the actual diskchecker with our dummy one
	testChecker = &dummyDiskChecker{
		c:      make(chan diskcheck.Health, 100),
		health: true,
	}
	newChecker = func(string, int, int, time.Duration) (diskcheck.Checker, error) { return testChecker, nil }

	defaultProcessor, err = New(store, 3, storageDir, logger)
	if err != nil {
		log.Fatal(err)
	}

	defaultWP = defaultProcessor.newWorkerPool(defaultAggr)

	exit := m.Run()

	server.Close()
	os.Exit(exit)
}

// Utility function that creates a job getting its ID and download URL using
// the name of the current test
func getTestJob(t *testing.T) job.Job {
	return job.Job{
		ID:          t.Name(),
		URL:         strings.Join([]string{server.URL, t.Name()}, "/"),
		AggrID:      defaultAggr.ID,
		CallbackURL: "http://example.com",
	}
}

// Utility function to dynamically create handlers with the specified name.
func addHandler(endpoint string, handler func(w http.ResponseWriter, r *http.Request)) {
	if !strings.HasPrefix(endpoint, "/") {
		endpoint = "/" + endpoint
	}
	mux.HandleFunc(endpoint, handler)
}

type dummyDiskChecker struct {
	health diskcheck.Health
	c      chan diskcheck.Health
}

func (d *dummyDiskChecker) Healthy() {
	if d.health == diskcheck.Healthy {
		return
	}
	d.health = diskcheck.Healthy
	d.c <- diskcheck.Healthy
}

func (d *dummyDiskChecker) Sick() {
	if d.health == diskcheck.Sick {
		return
	}
	d.health = diskcheck.Sick
	d.c <- diskcheck.Sick
}

func (d *dummyDiskChecker) C() chan diskcheck.Health {
	return d.c
}

func (d *dummyDiskChecker) Run(ctx context.Context) {
	<-ctx.Done()
}

func TestReaper(t *testing.T) {
	err := Redis.FlushDB().Err()
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
		defaultProcessor.reaper(ctx)
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

	defaultProcessor.collectRogueDownloads()

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

func TestChecker(t *testing.T) {
	err := Redis.FlushDB().Err()
	if err != nil {
		t.Fatal(err)
	}

	closeChan := make(chan struct{})
	go defaultProcessor.Start(closeChan)

	// At the beginning the disk health is "healthy" (our convention). By
	// calling Sick() we change its health to "sick".
	// At this state we test that no jobs will be processed.
	testChecker.Sick()
	time.Sleep(10 * time.Millisecond)

	a, _ := job.NewAggregation("foobar", 2)
	store.SaveAggregation(a)

	jobs := []job.Job{
		{
			ID:     "id1",
			AggrID: a.ID,
		},
		{
			ID:     "id2",
			AggrID: a.ID,
		},
	}

	for _, job := range jobs {
		err = store.QueuePendingDownload(&job, 0)
		if err != nil {
			t.Fatal(err)
		}
	}

	enqueueTime := fmt.Sprintf("%f", float64(time.Now().Unix()))
	time.Sleep(time.Second)

	jobCounter := func() int {

		// Using the enqueueTime as the max in ZCount we make sure that the
		// jobs have been processed at least once.
		c, err := store.Redis.ZCount(storage.JobsKeyPrefix+a.ID, "-inf", enqueueTime).Result()
		if err != nil {
			t.Fatal(err)
		}

		return int(c)
	}

	c := jobCounter()
	if c != len(jobs) {
		t.Fatalf("No jobs should have been processed. Expected: %d, got: %d", len(jobs), c)
	}

	// The disk health is now "sick" because we have used Sick() before to set
	// it. By calling Healthy() we will change the disk health to "healthy".
	// At this state we test that all jobs can be processed.
	testChecker.Healthy()

	for i := 0; i < 10; i++ {
		if c = jobCounter(); c == 0 {
			break
		}

		time.Sleep(time.Second)
	}

	if c != 0 {
		t.Fatalf("All the jobs should have been processed. Expected: %d, got: %d", 0, c)
	}

	closeChan <- struct{}{}
	<-closeChan
}

func TestJobWithNoAggregation(t *testing.T) {
	if err := Redis.FlushDB().Err(); err != nil {
		t.Fatal(err)
	}

	reqs := make(chan struct{}, 1)
	addHandler(t.Name(), func(w http.ResponseWriter, r *http.Request) {
		reqs <- struct{}{}
		w.WriteHeader(http.StatusOK)
	})

	testJob := getTestJob(t)
	//This aggregation hasn't been stored in Redis
	testJob.AggrID = "Aggr" + t.Name()

	err := store.QueuePendingDownload(&testJob, 0)
	if err != nil {
		t.Fatal(err)
	}
	closeChan := make(chan struct{})
	go defaultProcessor.Start(closeChan)

	select {
	// The job has been processed and a download request has been sent.
	case <-reqs:
	// The timeout represents the maximum allowed processing time for the job.
	case <-time.After(time.Second * 5):
		t.Fatal("Expected job to have been processed")
	}

	closeChan <- struct{}{}
	<-closeChan
}
