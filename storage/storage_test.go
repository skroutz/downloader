package storage

import (
	"fmt"
	"log"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/skroutz/downloader/config"
	"github.com/skroutz/downloader/job"
)

var (
	Redis   *redis.Client
	storage *Storage
	testCfg = "../config.test.json"
	testJob = job.Job{
		ID:          "TestJob",
		URL:         "http://localhost:12345",
		AggrID:      "TestAggr",
		CallbackURL: "http://callback.localhost:12345"}
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

	storage, err = New(Redis)
	if err != nil {
		log.Fatal(err)
	}
}

func TestSaveJob(t *testing.T) {
	Redis.FlushDB()

	err := storage.SaveJob(&testJob)
	if err != nil {
		t.Fatal(err)
	}

	job, err := storage.GetJob(testJob.ID)
	if err != nil {
		t.Fatal(err)
	}

	equalJobs := reflect.DeepEqual(testJob, job)
	if !equalJobs {
		t.Error("Jobs do not match!")
	}
}

func TestPendingJob(t *testing.T) {
	Redis.FlushDB()

	err := storage.SaveJob(&testJob)
	if err != nil {
		t.Fatal(err)
	}

	err = storage.QueuePendingDownload(&testJob, 0)
	if err != nil {
		t.Fatal(err)
	}

	aggr, err := job.NewAggregation(testJob.AggrID, 8, "", "")
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

func TestRetryCallback(t *testing.T) {
	Redis.FlushDB()

	testJob := job.Job{ID: "TestJob", CallbackState: job.StateFailed,
		CallbackURL: "http://callback.localhost:12345"}

	err := storage.SaveJob(&testJob)
	if err != nil {
		t.Fatal(err)
	}

	err = storage.RetryCallback(&testJob)
	if err != nil {
		t.Fatal(err)
	}

	queuedJob, err := storage.PopCallback()
	if err != nil {
		t.Fatal(err)
	}
	if queuedJob.ID != testJob.ID {
		t.Error("Expected to have injected the job to the callback queue")
	}

	if queuedJob.CallbackState != job.StatePending {
		t.Errorf("Expected callback state: %s, got: %s",
			job.StatePending, queuedJob.CallbackState)
	}
	if queuedJob.CallbackCount != 0 {
		t.Errorf("Expected callback count to be: %d, got: %d",
			0, queuedJob.CallbackCount)
	}
}

func TestRemoveAggregationWithNoJobs(t *testing.T) {
	Redis.FlushDB()

	testAggr, _ := job.NewAggregation(testJob.AggrID, 8, "", "")
	storage.SaveAggregation(testAggr)
	err := storage.RemoveAggregation(testAggr.ID)
	if err != nil {
		t.Fatal(err)
	}
	exists, _ := storage.AggregationExists(testAggr)
	if exists {
		t.Error("Expected aggregation to have been deleted", err)
	}
}

func TestRemoveAggregationWithJobs(t *testing.T) {
	Redis.FlushDB()

	testAggr, _ := job.NewAggregation(testJob.AggrID, 8, "", "")
	storage.SaveAggregation(testAggr)
	storage.QueuePendingDownload(&testJob, 0)

	err := storage.RemoveAggregation(testAggr.ID)
	if err != nil {
		t.Fatal(err)
	}
	exists, _ := storage.AggregationExists(testAggr)
	if !exists {
		t.Error("Expected aggregation to exist", err)
	}
}

func TestGetAggregation(t *testing.T) {
	Redis.FlushDb()

	existingAggr, _ := job.NewAggregation("existingID", 8, "", "")
	storage.SaveAggregation(existingAggr)
	testCases := []string{
		existingAggr.ID,
		"nonExistingID",
	}

	for _, id := range testCases {
		t.Run(fmt.Sprintf("%s", id), func(t *testing.T) {
			_, err := storage.GetAggregation(id)
			if err != ErrNotFound && err != nil {
				t.Error("Expected to fetch the aggregation", err)
			}
		})
	}
}

func TestAggregationExpiry(t *testing.T) {
	Redis.FlushDb()

	now := strconv.FormatInt(time.Now().Unix(), 10)
	processedAggr, _ := job.NewAggregation("processedAggr", 1, "", now)
	expiredAggr, _ := job.NewAggregation("expiredAggr", 1, "", "1")
	storage.SaveAggregation(processedAggr)
	storage.SaveAggregation(expiredAggr)

	t.Run(processedAggr.ID, func(t *testing.T) {
		_, err := storage.GetAggregation(processedAggr.ID)
		if err != ErrAggrBeingProcessed {
			t.Error("Expected to skip aggregation being processed", err)
		}
	})

	t.Run(expiredAggr.ID, func(t *testing.T) {
		_, err := storage.GetAggregation(expiredAggr.ID)
		if err != nil {
			t.Error("Expected to fetch expired aggregation", err)
		}
	})

}
