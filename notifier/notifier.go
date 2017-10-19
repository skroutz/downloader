package notifier

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"golang.skroutz.gr/skroutz/downloader/job"
	"golang.skroutz.gr/skroutz/downloader/storage"
)

const maxCallbackRetries = 2

// callbackInfo holds the info to be posted back to the provided callback url of the caller
type callbackInfo struct {
	Success     bool   `json:"success"`
	Error       string `json:"error"`
	Extra       string `json:"extra"`
	DownloadURL string `json:"download_url"`
}

// Notifier is the the component responsible for consuming the result of jobs
// and notifying back the respective users by issuing HTTP requests to their
// provided callback URLs.
type Notifier struct {
	Storage *storage.Storage

	// TODO: These should be exported
	concurrency int
	client      *http.Client
	cbChan      chan job.Job
}

// NewNotifier takes the concurrency of the notifier as an argument
//
// TODO: check concurrency is > 0
func New(s *storage.Storage, concurrency int) Notifier {
	return Notifier{
		Storage:     s,
		concurrency: concurrency,
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{},
			},
			Timeout: time.Duration(3) * time.Second,
		},
		cbChan: make(chan job.Job),
	}
}

// Start starts the Notifier loop and instruments the worker goroutines that
// perform the actual notify requests.
func (n *Notifier) Start(closeChan chan struct{}) {
	var wg sync.WaitGroup
	wg.Add(n.concurrency)
	for i := 0; i < n.concurrency; i++ {
		go func() {
			defer wg.Done()
			for job := range n.cbChan {
				n.Notify(&job)
			}

		}()
	}

	for {
		select {
		case <-closeChan:
			close(n.cbChan)
			wg.Wait()
			closeChan <- struct{}{}
			return
		default:
			job, err := n.Storage.PopCallback()
			if err != nil {
				if _, ok := err.(storage.QueueEmptyError); ok {
					time.Sleep(time.Second)
				} else {
					log.Println(err)
				}
				continue
			}
			n.cbChan <- job
		}
	}
}

// Notify posts callback info to the Job's CallbackURL
// using the provided http.Client
func (n *Notifier) Notify(j *job.Job) {
	n.Storage.UpdateCallbackState(j, job.StateInProgress)
	cbInfo, err := getCallbackInfo(j)
	if err != nil {
		n.Storage.UpdateDownloadState(j, job.StateFailed, err.Error())
		return
	}

	cb, err := json.Marshal(cbInfo)
	if err != nil {
		n.Storage.UpdateDownloadState(j, job.StateFailed, err.Error())
		return
	}

	res, err := n.client.Post(j.CallbackURL, "application/json", bytes.NewBuffer(cb))
	if err != nil || res.StatusCode < 200 || res.StatusCode >= 300 {
		if err == nil {
			err = fmt.Errorf("Received Status: %s", res.Status)
		}
		n.Storage.UpdateCallbackState(j, job.StateFailed, err.Error())
		return
	}

	n.Storage.UpdateCallbackState(j, job.StateSuccess)
}

// retryOrFail checks the callback count of the current download
// and retries the callback if its Retry Counts < maxRetries else it marks
// it as failed
//
// TODO: isn't used anywhere. Why?
func (n *Notifier) retryOrFail(j *job.Job, err string) error {
	if j.CallbackCount >= maxCallbackRetries {
		return n.Storage.UpdateCallbackState(j, job.StateFailed, err)
	}
	j.CallbackCount++
	return n.Storage.QueuePendingCallback(j)
}

// callbackInfo validates that the job is good for callback and
// return callbackInfo to the caller
func getCallbackInfo(j *job.Job) (callbackInfo, error) {
	if j.DownloadState != job.StateSuccess && j.DownloadState != job.StateFailed {
		return callbackInfo{}, fmt.Errorf("Invalid Job State %s", j.DownloadState)
	}

	return callbackInfo{
		Success:     j.DownloadState == job.StateSuccess,
		Error:       j.Meta,
		Extra:       j.Extra,
		DownloadURL: jobDownloadURL(j),
	}, nil
}

// downloadURL constructs the actual download URL to be provided to the user.
//
// TODO: Actually make it smart
func jobDownloadURL(j *job.Job) string {
	return fmt.Sprintf("http://localhost/%s", j.ID)
}
