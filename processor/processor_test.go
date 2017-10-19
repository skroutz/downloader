package processor

//const savedir = "./"
//
//func init() {
//	InitStorage("127.0.0.1", 6379)
//	Redis.FlushAll()
//}
//
//func TestProcessorFlow(t *testing.T) {
//	job := GetTestJob()
//	job.URL = "https://httpbin.org/image/png"
//	a := Aggregation{ID: job.AggrID, Limit: 1}
//	a.Save()
//
//	err := job.QueuePendingDownload()
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	logger := log.New(os.Stderr, "[Processor] ", log.Ldate|log.Ltime)
//	client := &http.Client{
//		Transport: &http.Transport{TLSClientConfig: &tls.Config{}},
//		Timeout:   time.Duration(3) * time.Second}
//
//	processor, err := NewProcessor(1, savedir, client, logger)
//	if err != nil {
//		t.Fatal(err)
//	}
//	closeChan := make(chan struct{})
//
//	go processor.Start(closeChan)
//	time.Sleep(2 * time.Second)
//	closeChan <- struct{}{}
//	<-closeChan
//
//	defer os.Remove(savedir + job.ID)
//
//	job, err = GetJob(job.ID)
//	if err != nil {
//		t.Fatalf("Could not retrieve job from redis: %v", err)
//	}
//	if job.DownloadState != StateSuccess {
//		t.Error("Download was not successful")
//	}
//}
//
//func TestWorkerPoolWork(t *testing.T) {
//	processor, err := NewProcessor(1, savedir)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	job := GetTestJob()
//	job.URL = "https://httpbin.org/image/png"
//	job.Save()
//	defer os.Remove(savedir + job.ID)
//
//	wp := processor.newWorkerPool(Aggregation{ID: job.AggrID, Limit: 1})
//	var wg sync.WaitGroup
//	wg.Add(1)
//	go func() {
//		wp.work(context.TODO(), savedir)
//		wg.Done()
//	}()
//	wp.jobChan <- job
//	wg.Wait()
//	defer os.Remove(savedir + job.ID)
//	test, err := GetJob(job.ID)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	if test.DownloadState != StateSuccess {
//		t.Error("Job should not have failed")
//	}
//}

// TODO: brought from storage
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

// TODO: brought from storage
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
