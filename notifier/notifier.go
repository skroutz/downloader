package notifier

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.skroutz.gr/skroutz/downloader/job"
	"golang.skroutz.gr/skroutz/downloader/storage"
)

const maxCallbackRetries = 2

// CallbackInfo holds info to be posted back to the provided callback url.
type CallbackInfo struct {
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

	// Check Redis for jobs left in InProgress state
	n.collectRogueCallbacks()

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

// collectRogueCallbacks Scans Redis for jobs that have InProgress CallbackState.
// This indicates they are leftover from an interrupted previous run and should get requeued.
func (n *Notifier) collectRogueCallbacks() {
	var cursor uint64
	var rogueCount uint64

	for {
		var keys []string
		var err error
		keys, cursor, err = n.Storage.Redis.Scan(cursor, storage.JobKeyPrefix+"*", 50).Result()
		if err != nil {
			log.Println(err)
			break
		}

		for _, jobID := range keys {
			strCmd := n.Storage.Redis.HGet(jobID, "CallbackState")
			if strCmd.Err() != nil {
				log.Println(strCmd.Err())
				continue
			}
			if job.State(strCmd.Val()) == job.StateInProgress {
				jb, err := n.Storage.GetJob(strings.TrimPrefix(jobID, storage.JobKeyPrefix))
				if err != nil {
					log.Printf("Could not get job for Redis: %v", err)
					continue
				}
				err = n.Storage.QueuePendingCallback(&jb)
				if err != nil {
					log.Printf("Could not queue job for download: %v", err)
					continue
				}
				rogueCount++
			}
		}

		if cursor == 0 {
			break
		}
	}
	log.Printf("Queued %d Rogue Callbacks", rogueCount)
}

// Notify posts callback info to j.CallbackURL
func (n *Notifier) Notify(j *job.Job) {
	n.markCbInProgress(j)

	cbInfo, err := getCallbackInfo(j)
	if err != nil {
		n.markCbFailed(j, err.Error())
		return
	}

	cb, err := json.Marshal(cbInfo)
	if err != nil {
		n.markCbFailed(j, err.Error())
		return
	}

	res, err := n.client.Post(j.CallbackURL, "application/json", bytes.NewBuffer(cb))
	if err != nil || res.StatusCode < 200 || res.StatusCode >= 300 {
		if err == nil {
			err = fmt.Errorf("Received Status: %s", res.Status)
		}
		n.retryOrFail(j, err.Error())
		return
	}

	n.Storage.RemoveJob(j.ID)
}

// retryOrFail checks the callback count of the current download
// and retries the callback if its Retry Counts < maxRetries else it marks
// it as failed
func (n *Notifier) retryOrFail(j *job.Job, err string) error {
	if j.CallbackCount >= maxCallbackRetries {
		return n.markCbFailed(j, err)
	}
	j.CallbackCount++
	return n.Storage.QueuePendingCallback(j)
}

// callbackInfo validates that the job is good for callback and
// return callbackInfo to the caller
func getCallbackInfo(j *job.Job) (CallbackInfo, error) {
	if j.DownloadState != job.StateSuccess && j.DownloadState != job.StateFailed {
		return CallbackInfo{}, fmt.Errorf("Invalid job download state: '%s'", j.DownloadState)
	}

	return CallbackInfo{
		Success:     j.DownloadState == job.StateSuccess,
		Error:       j.DownloadMeta,
		Extra:       j.Extra,
		DownloadURL: jobDownloadURL(j),
	}, nil
}

// downloadURL constructs the actual download URL to be provided to the user.
//
// TODO: Actually make it smart
func jobDownloadURL(j *job.Job) string {
	if j.DownloadState != job.StateSuccess {
		return ""
	}
	return fmt.Sprintf("http://localhost/%s", j.ID)
}

func (n *Notifier) markCbInProgress(j *job.Job) error {
	j.CallbackState = job.StateInProgress
	j.CallbackMeta = ""
	return n.Storage.SaveJob(j)
}

func (n *Notifier) markCbFailed(j *job.Job, meta ...string) error {
	j.CallbackState = job.StateFailed
	j.CallbackMeta = strings.Join(meta, "\n")
	return n.Storage.SaveJob(j)
}
