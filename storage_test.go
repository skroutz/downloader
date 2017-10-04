package main

import (
	"testing"
)

func init() {
	InitStorage("127.0.0.1", 6379)
	Redis.FlushAll()
}

func GetTestJob() Job {
	return Job{
		ID:          "TestJob",
		URL:         "http://localhost:12345",
		AggrID:      "TestAggr",
		CallbackURL: "http://callback.localhost:12345",
	}
}

func TestSave(t *testing.T) {
	job := GetTestJob()
	job.Save()
	testjob, err := GetJob(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if testjob != job {
		t.Error("Jobs do not match!")
	}
}

func TestPending(t *testing.T) {
	job := GetTestJob()
	job.Save()
	err := job.QueuePendingDownload()
	if err != nil {
		t.Fatal(err)
	}

	testjob, err := PopJob(jobKeyPrefix + job.AggrID)
	if err != nil {
		t.Fatal(err)
	}

	if testjob.DownloadState != StatePending {
		t.Error("Invalid Download State")
	}

	if testjob.ID != job.ID {
		t.Error("Wrong job popped")
	}
}

func TestRetryAndFail(t *testing.T) {
	job := GetTestJob()
	job.Save()
	for i := 0; i <= maxRetries; i++ {
		err := job.RetryOrFail("Test")
		if err != nil {
			t.Fatal(err)
		}

		if job.DownloadState != StateFailed {
			job, err = PopJob(jobKeyPrefix + job.AggrID)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	testjob, err := GetJob(job.ID)
	if err != nil {
		t.Fatal(err)
	}

	if testjob.DownloadState != StateFailed {
		t.Error("Job should have failed state")
	}
}
