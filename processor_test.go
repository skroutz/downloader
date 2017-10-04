package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

const savedir = "./"

func init() {
	InitStorage("127.0.0.1", 6379)
	Redis.FlushAll()
}

func TestProcessorOpenClose(t *testing.T) {
	processor := &Processor{StorageDir: savedir}
	closechan := make(chan struct{})
	go processor.Start(closechan)

	time.Sleep(1)
	closechan <- struct{}{}
	<-closechan
}

func TestProcessorFlow(t *testing.T) {
	job := GetTestJob()
	job.URL = "https://httpbin.org/image/png"
	a := Aggregation{ID: job.AggrID, Limit: 1}
	a.Save()

	err := job.QueuePendingDownload()
	if err != nil {
		t.Fatal(err)
	}

	processor := NewProcessor(savedir)
	closeChan := make(chan struct{})

	go processor.Start(closeChan)
	time.Sleep(2 * time.Second)
	closeChan <- struct{}{}
	<-closeChan

	defer os.Remove(savedir + job.ID)

	job, err = GetJob(job.ID)
	if err != nil {
		t.Fatalf("Could not retrieve job from redis: %v", err)
	}
	if job.DownloadState != StateSuccess {
		t.Error("Download was not successful")
	}
}

func TestDownload(t *testing.T) {
	job := GetTestJob()
	job.URL = "https://httpbin.org/image/png"
	job.Save()
	defer os.Remove(savedir + job.ID)

	wp := NewWorkerPool(Aggregation{ID: job.AggrID, Limit: 1})
	wp.jobChan <- job
	wp.work(context.TODO(), savedir)
	defer os.Remove(savedir + job.ID)
	test, err := GetJob(job.ID)
	if err != nil {
		t.Fatal(err)
	}

	if test.DownloadState != StateSuccess {
		t.Error("Job should not have failed")
	}
}

func TestTLSError(t *testing.T) {
	job := GetTestJob()
	job.URL = "https://expired.badssl.com"
	job.Save()
	wp := NewWorkerPool(Aggregation{ID: job.AggrID, Limit: 1})
	wp.jobChan <- job
	wp.work(context.TODO(), savedir)
	defer os.Remove(savedir + job.ID)

	j, err := GetJob(job.ID)
	if err != nil {
		t.Fatalf("Could not retrieve job from redis: %v", err)
	}

	if j.DownloadState != StateFailed {
		t.Error("Download should have failed")
	}
	if !strings.Contains(j.Meta, "TLS Error occured") {
		t.Error("TLS Error was not reported correctly")
	}
	if j.RetryCount > 0 {
		t.Error("TLS Errors should not be retried")
	}
}
