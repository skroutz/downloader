package storage

//func init() {
//	InitStorage("127.0.0.1", 6379)
//	Redis.FlushAll()
//}
//
//func GetTestJob() Job {
//	return Job{
//		ID:          "TestJob",
//		URL:         "http://localhost:12345",
//		AggrID:      "TestAggr",
//		CallbackURL: "http://callback.localhost:12345",
//	}
//}
//
//func TestSave(t *testing.T) {
//	job := GetTestJob()
//	job.Save()
//	testjob, err := GetJob(job.ID)
//	if err != nil {
//		t.Fatal(err)
//	}
//	if testjob != job {
//		t.Error("Jobs do not match!")
//	}
//}
//
//func TestPending(t *testing.T) {
//	job := GetTestJob()
//	job.Save()
//	err := job.QueuePendingDownload()
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	aggr := Aggregation{ID: job.AggrID}
//	testjob, err := aggr.PopJob()
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	if testjob.DownloadState != StatePending {
//		t.Error("Invalid Download State")
//	}
//
//	if testjob.ID != job.ID {
//		t.Error("Wrong job popped")
//	}
//}
//
//func TestRetryAndFail(t *testing.T) {
//	job := GetTestJob()
//	job.Save()
//	aggr := Aggregation{ID: job.AggrID}
//
//	for i := 0; i <= MaxDownloadRetries; i++ {
//		err := job.requeueOrFail("Test")
//		if err != nil {
//			t.Fatal(err)
//		}
//
//		if job.DownloadState != StateFailed {
//			job, err = aggr.PopJob()
//			if err != nil {
//				t.Fatal(err)
//			}
//		}
//	}
//
//	testjob, err := GetJob(job.ID)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	if testjob.DownloadState != StateFailed {
//		t.Error("Job should have failed state")
//	}
//}
//
//func TestTLSError(t *testing.T) {
//	job := GetTestJob()
//	job.URL = "https://expired.badssl.com"
//	job.Save()
//	job.Perform(context.TODO(), savedir)
//	defer os.Remove(savedir + job.ID)
//
//	j, err := GetJob(job.ID)
//	if err != nil {
//		t.Fatalf("Could not retrieve job from redis: %v", err)
//	}
//
//	if j.DownloadState != StateFailed {
//		t.Error("Download should have failed")
//	}
//	if !strings.Contains(j.Meta, "TLS Error occured") {
//		t.Error("TLS Error was not reported correctly")
//	}
//	if j.RetryCount > 0 {
//		t.Error("TLS Errors should not be retried")
//	}
//}
