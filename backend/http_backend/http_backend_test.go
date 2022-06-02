package httpbackend

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/skroutz/downloader/config"
	"github.com/skroutz/downloader/job"
)

var (
	cbServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	httpB *Backend
	ctx   context.Context
	jobS  *job.Job
	dwURL *url.URL
)

func init() {
	ctx = context.Background()
	dwURL, _ = url.Parse("http://blah.com/")
	jobS = &job.Job{
		ID:            "successjob",
		URL:           "http://localhost:12345",
		AggrID:        "notifoo",
		DownloadState: job.StateSuccess,
		CallbackURL:   cbServer.URL,
		ResponseCode:  200,
	}
}

func TestHttpBackendNotifySucess(t *testing.T) {
	var wg sync.WaitGroup

	testCfgFile := "../../config.test.json"
	cfg, err := config.Parse(testCfgFile)
	if err != nil {
		t.Fatalf("Could not load test configuration %s. Operation returned %s", testCfgFile, err)
	}

	httpB = &Backend{}

	err = httpB.Start(ctx, cfg.Backends["http"])
	if err != nil {
		t.Fatalf("Start should not return error")
	}

	cbInfoS, _ := jobS.CallbackInfo()

	wg.Add(1)
	go func() {
		err := httpB.Notify(jobS.CallbackURL, cbInfoS)
		if err != nil {
			t.Fatalf("Expected Notify to be successful")
		}
		wg.Done()
	}()

	time.Sleep(2 * time.Second)

	cbInfo := <-httpB.DeliveryReports()
	if !cbInfo.Delivered {
		t.Fatalf("Expected callback delivery to be successful")
	}

	err = httpB.Stop()
	if err != nil {
		t.Fatalf("Error while finalizing %s ", err)
	}
	wg.Wait()
}
