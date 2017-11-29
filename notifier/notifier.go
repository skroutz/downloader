package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"golang.skroutz.gr/skroutz/downloader/job"
	"golang.skroutz.gr/skroutz/downloader/stats"
	"golang.skroutz.gr/skroutz/downloader/storage"
)

const (
	maxCallbackRetries = 2

	statsFailedCallbacks     = "failedCallbacks"     //Counter
	statsSuccessfulCallbacks = "successfulCallbacks" //Counter
)

var (
	// expvar.Publish() panics if a name is already registered, hence
	// we need to be able to override it in order to test Notifier easily.
	// TODO: we should probably get rid of expvar to avoid such issues
	statsID = "Notifier"

	// Based on http.DefaultTransport
	//
	// See https://golang.org/pkg/net/http/#RoundTripper
	notifierTransport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second, // was 30 * time.Second
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
)

// CallbackInfo holds info to be posted back to the provided callback url.
type CallbackInfo struct {
	Success      bool   `json:"success"`
	Error        string `json:"error"`
	Extra        string `json:"extra"`
	ResourceURL  string `json:"resource_url"`
	DownloadURL  string `json:"download_url"`
	JobID        string `json:"job_id"`
	ResponseCode int    `json:"response_code"`
}

// Notifier is the the component responsible for consuming the result of jobs
// and notifying back the respective users by issuing HTTP requests to their
// provided callback URLs.
type Notifier struct {
	Storage     *storage.Storage
	Log         *log.Logger
	DownloadURL *url.URL
	StatsIntvl  time.Duration

	// TODO: These should be exported
	concurrency int
	client      *http.Client
	cbChan      chan job.Job
	stats       *stats.Stats
}

// NewNotifier takes the concurrency of the notifier as an argument
func New(s *storage.Storage, concurrency int, logger *log.Logger, dwnlURL string) (Notifier, error) {
	url, err := url.ParseRequestURI(dwnlURL)
	if err != nil {
		return Notifier{}, fmt.Errorf("Could not parse Download URL, %v", err)
	}

	if concurrency <= 0 {
		return Notifier{}, errors.New("Notifier Concurrency must be a positive number")
	}

	n := Notifier{
		Storage:     s,
		Log:         logger,
		StatsIntvl:  5 * time.Second,
		concurrency: concurrency,
		client: &http.Client{
			Transport: notifierTransport,
			Timeout:   30 * time.Second, // Larger than Dial + TLS timeouts
		},
		cbChan:      make(chan job.Job),
		DownloadURL: url,
	}

	n.stats = stats.New(statsID, n.StatsIntvl, func(m *expvar.Map) {
		// Store metrics in JSON
		err := n.Storage.SetStats("notifier", m.String(), 2*n.StatsIntvl)
		if err != nil {
			n.Log.Println("Could not report stats", err)
		}
	})

	return n, nil
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
				err := n.Notify(&job)
				if err != nil {
					n.Log.Printf("Notify error: %s", err)
				}
			}
		}()
	}

	// Check Redis for jobs left in InProgress state
	n.collectRogueCallbacks()

	ctx, cancelfunc := context.WithCancel(context.Background())
	go n.stats.Run(ctx)

	for {
		select {
		case <-closeChan:
			close(n.cbChan)
			wg.Wait()
			cancelfunc()
			closeChan <- struct{}{}
			return
		default:
			job, err := n.Storage.PopCallback()
			if err != nil {
				switch err {
				case storage.ErrEmptyQueue:
					// noop
				case storage.ErrRetryLater:
					// noop
				default:
					n.Log.Println(err)
				}

				time.Sleep(time.Second)
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
			n.Log.Println(err)
			break
		}

		for _, jobID := range keys {
			strCmd := n.Storage.Redis.HGet(jobID, "CallbackState")
			if strCmd.Err() != nil {
				n.Log.Println(strCmd.Err())
				continue
			}
			if job.State(strCmd.Val()) == job.StateInProgress {
				jb, err := n.Storage.GetJob(strings.TrimPrefix(jobID, storage.JobKeyPrefix))
				if err != nil {
					n.Log.Printf("Could not get job for Redis: %v", err)
					continue
				}
				err = n.Storage.QueuePendingCallback(&jb)
				if err != nil {
					n.Log.Printf("Could not queue job for download: %v", err)
					continue
				}
				rogueCount++
			}
		}

		if cursor == 0 {
			break
		}
	}

	if rogueCount > 0 {
		n.Log.Printf("Queued %d rogue callbacks", rogueCount)
	}
}

// Notify posts callback info to j.CallbackURL
func (n *Notifier) Notify(j *job.Job) error {
	j.CallbackCount++

	err := n.markCbInProgress(j)
	if err != nil {
		return err
	}

	cbInfo, err := n.getCallbackInfo(j)
	if err != nil {
		return n.markCbFailed(j, err.Error())
	}

	cb, err := json.Marshal(cbInfo)
	if err != nil {
		return n.markCbFailed(j, err.Error())
	}

	res, err := n.client.Post(j.CallbackURL, "application/json", bytes.NewBuffer(cb))
	if err != nil || res.StatusCode < 200 || res.StatusCode >= 300 {
		if err == nil {
			err = fmt.Errorf("Received Status: %s", res.Status)
		}
		return n.retryOrFail(j, err.Error())
	}

	if res.StatusCode == http.StatusAccepted {
		err := n.Storage.QueueJobForDeletion(j.ID)
		if err != nil {
			n.Log.Println("Error: Could not queue job for deletion", err)
		}
	}

	n.stats.Add(statsSuccessfulCallbacks, 1)
	return n.Storage.RemoveJob(j.ID)
}

// retryOrFail checks the callback count of the current download
// and retries the callback if its Retry Counts < maxRetries else it marks
// it as failed
func (n *Notifier) retryOrFail(j *job.Job, err string) error {
	if j.CallbackCount >= maxCallbackRetries {
		return n.markCbFailed(j, err)
	}

	n.Log.Printf("Warn: Callback try no:%d failed for job:%s with: %s", j.CallbackCount, j, err)
	return n.Storage.QueuePendingCallback(j)
}

// callbackInfo validates that the job is good for callback and
// return callbackInfo to the caller
func (n *Notifier) getCallbackInfo(j *job.Job) (CallbackInfo, error) {
	if j.DownloadState != job.StateSuccess && j.DownloadState != job.StateFailed {
		return CallbackInfo{}, fmt.Errorf("Invalid job download state: '%s'", j.DownloadState)
	}

	return CallbackInfo{
		Success:      j.DownloadState == job.StateSuccess,
		Error:        j.DownloadMeta,
		Extra:        j.Extra,
		ResourceURL:  j.URL,
		DownloadURL:  jobDownloadURL(j, *n.DownloadURL),
		JobID:        j.ID,
		ResponseCode: j.ResponseCode,
	}, nil
}

// jobdownloadURL constructs the actual download URL to be provided to the user.
func jobDownloadURL(j *job.Job, downloadURL url.URL) string {
	if j.DownloadState != job.StateSuccess {
		return ""
	}

	downloadURL.Path = path.Join(downloadURL.Path, j.Path())
	return downloadURL.String()
}

func (n *Notifier) markCbInProgress(j *job.Job) error {
	j.CallbackState = job.StateInProgress
	j.CallbackMeta = ""
	return n.Storage.SaveJob(j)
}

func (n *Notifier) markCbFailed(j *job.Job, meta ...string) error {
	j.CallbackState = job.StateFailed
	j.CallbackMeta = strings.Join(meta, "\n")
	n.Log.Printf("Error: Callback for %s failed: %s", j, j.CallbackMeta)

	//Report stats
	n.stats.Add(statsFailedCallbacks, 1)
	return n.Storage.SaveJob(j)
}
