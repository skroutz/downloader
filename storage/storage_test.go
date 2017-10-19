package storage

import (
	"log"
	"testing"

	"github.com/go-redis/redis"
	"golang.skroutz.gr/skroutz/downloader/job"
)

var (
	Redis   *redis.Client
	storage *Storage
	testJob = job.Job{
		ID:          "TestJob",
		URL:         "http://localhost:12345",
		AggrID:      "TestAggr",
		CallbackURL: "http://callback.localhost:12345"}
)

func init() {
	var err error

	Redis = redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	storage, err = New(Redis)
	if err != nil {
		log.Fatal(err)
	}

	Redis.FlushDB()
}

func TestSaveJob(t *testing.T) {
	err := storage.SaveJob(&testJob)
	if err != nil {
		t.Fatal(err)
	}

	job, err := storage.GetJob(testJob.ID)
	if err != nil {
		t.Fatal(err)
	}

	if testJob != job {
		t.Error("Jobs do not match!")
	}
}

func TestPendingJob(t *testing.T) {
	err := storage.SaveJob(&testJob)
	if err != nil {
		t.Fatal(err)
	}

	err = storage.QueuePendingDownload(&testJob)
	if err != nil {
		t.Fatal(err)
	}

	aggr, err := job.NewAggregation(testJob.AggrID, 8)
	if err != nil {
		t.Fatal(err)
	}

	poppedJob, err := storage.PopJob(aggr)
	if err != nil {
		t.Fatal(err)
	}

	if poppedJob.DownloadState != job.StatePending {
		t.Error("Invalid Download State")
	}

	if testJob.ID != poppedJob.ID {
		t.Error("Wrong job popped")
	}
}
