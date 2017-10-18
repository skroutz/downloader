package main

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
