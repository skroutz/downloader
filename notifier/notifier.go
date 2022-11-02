package notifier

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/skroutz/downloader/backend"
	httpbackend "github.com/skroutz/downloader/backend/http_backend"
	kafkabackend "github.com/skroutz/downloader/backend/kafka_backend"
	sqsbackend "github.com/skroutz/downloader/backend/sqs_backend"
	"github.com/skroutz/downloader/job"
	"github.com/skroutz/downloader/stats"
	"github.com/skroutz/downloader/storage"
)

const (
	maxCallbackRetries                = 2
	statsFailedCallbacks              = "failedCallbacks"              //Counter
	statsSuccessfulCallbacks          = "successfulCallbacks"          //Counter
	statsUndefinedBackendWithCallback = "undefinedBackendWithCallback" //Counter
	statsUnknownTopicOrPartition      = "unknownTopicOrPartition"      //Counter

	// BackendHTTPID is a known backend implementation
	BackendHTTPID = "http"

	// BackendKafkaID is a known backend implementation
	BackendKafkaID = "kafka"

	// BackendSqsID is a known backend implementation
	BackendSQSID = "sqs"
)

var (
	// expvar.Publish() panics if a name is already registered, hence
	// we need to be able to override it in order to test Notifier easily.
	// TODO: we should probably get rid of expvar to avoid such issues
	statsID = "Notifier"

	// RetryBackoffDuration indicates the time to wait between retries.
	RetryBackoffDuration = 10 * time.Minute
)

// Notifier is the the component responsible for consuming the result of jobs
// and notifying back the respective users by issuing HTTP requests to their
// provided callback URLs.
type Notifier struct {
	Storage    *storage.Storage
	Log        *log.Logger
	StatsIntvl time.Duration

	// DeletionIntvl indicates the time after which downloaded files must be
	// enqueued for deletion.
	DeletionIntvl time.Duration

	// TODO: These should be exported
	concurrency int
	client      *http.Client
	cbChan      chan job.Job
	stats       *stats.Stats

	// registered backends
	backends map[string]backend.Backend
}

func init() {
	// Indicates we are in test mode
	if _, testMode := os.LookupEnv("DOWNLOADER_TEST_TIME"); testMode {
		RetryBackoffDuration = 200 * time.Millisecond
	}
}

// New takes the concurrency of the notifier as an argument
func New(s *storage.Storage, concurrency int, logger *log.Logger) (Notifier, error) {
	if concurrency <= 0 {
		return Notifier{}, errors.New("Notifier Concurrency must be a positive number")
	}

	n := Notifier{
		Storage:     s,
		Log:         logger,
		StatsIntvl:  5 * time.Second,
		concurrency: concurrency,
		cbChan:      make(chan job.Job),
		backends:    make(map[string]backend.Backend),
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
func (n *Notifier) Start(closeChan chan struct{}, backendCfg map[string]map[string]interface{}) {
	ctx, cancelfunc := context.WithCancel(context.Background())

	for id, v := range backendCfg {
		if len(v) == 0 {
			continue
		}

		if id == BackendHTTPID {
			n.backends[id] = &httpbackend.Backend{}
		} else if id == BackendKafkaID {
			n.backends[id] = &kafkabackend.Backend{}
		} else if id == BackendSQSID {
			n.backends[id] = &sqsbackend.Backend{}
		}

		b := n.backends[id]
		n.Log.Printf("Starting %s backend", b.ID())
		err := b.Start(ctx, v)
		if err != nil {
			n.Log.Fatalf("Error while initializing backend %s. Error details: %s", b.ID(), err)
		}
	}

	if len(n.backends) == 0 {
		n.Log.Fatalf("No backends are enabled. Configuration map given is %s", backendCfg)
	}

	var wg sync.WaitGroup
	wg.Add(n.concurrency)
	for i := 0; i < n.concurrency; i++ {
		go func() {
			defer wg.Done()
			for job := range n.cbChan {
				cbInfo, err := n.PreNotify(&job)
				if err != nil {
					n.Log.Printf("Error while preparing callback info: %s", err)
					continue
				}

				err = n.Notify(&job, cbInfo)
				if err != nil {
					n.Log.Printf("Notify error: %s", err)
				}
			}
		}()
	}

	// Check Redis for jobs left in InProgress state
	n.collectRogueCallbacks()

	go n.stats.Run(ctx)

	// Start monitoring delivery reports for each backend
	var deliveriesWg sync.WaitGroup
	for id := range n.backends {
		deliveriesWg.Add(1)
		go func(id string) {
			defer deliveriesWg.Done()
			n.monitorDeliveries(ctx, id)
		}(id)
	}

	for {
		select {
		case <-closeChan:
			close(n.cbChan)
			wg.Wait()
			cancelfunc()
			for _, b := range n.backends {
				n.Log.Printf("Closing %s backend", b.ID())
				err := b.Stop()
				if err != nil {
					n.Log.Printf("Error %s while finalizing backend %s", err, b.ID())
				}
			}
			deliveriesWg.Wait()
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
				err = n.Storage.QueuePendingCallback(&jb, 0)
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

// monitorDeliveries consumes callback objects from the running backends
// and handles them appropriately. The backendID is a string representing
// the id of a backend. See also n.handleCallbackInfo().
func (n *Notifier) monitorDeliveries(ctx context.Context, backendID string) {
	for {
		select {
		case <-ctx.Done():
			return
		case cbInfo, ok := <-n.backends[backendID].DeliveryReports():
			if !ok {
				return
			}

			err := n.handleCallbackInfo(cbInfo)
			if err != nil {
				n.Log.Printf("Error occured during handling callback info. Operation returned error %s", err)
			}
		}
	}
}

// handleCallbackInfo handles a callback's delivery result.
// If the callback has been successfully delivered we increment the appropriate
// stats counters and remove the job from storage.
// Otherwise we mark the callback as failed.
func (n *Notifier) handleCallbackInfo(cbInfo job.Callback) error {
	j, err := n.Storage.GetJob(cbInfo.JobID)
	if err != nil {
		return fmt.Errorf("\nError: Could not get job %s. Operation returned error: %s", cbInfo.JobID, err)
	}

	if cbInfo.Delivered {
		n.stats.Add(statsSuccessfulCallbacks, 1)

		err := n.Storage.RemoveJob(cbInfo.JobID)
		if err != nil {
			return fmt.Errorf("Could not remove job %s. Operation returned error: %s", cbInfo.JobID, err)
		}

		err = n.Storage.QueueJobForDeletion(cbInfo.JobID, n.DeletionIntvl)
		if err != nil {
			return fmt.Errorf("Error: Could not queue job for deletion %s", err)
		}

		return nil
	}

	if cbInfo.DeliveryError != "" {
		if cbInfo.DeliveryError == "Broker: Unknown topic or partition" {
			n.stats.Add(statsUnknownTopicOrPartition, 1)
		}

		err = n.markCbFailed(&j, cbInfo.DeliveryError)
		if err != nil {
			return fmt.Errorf(
				"\nError during marking callback as failed for job %s. Operation returned error: %s",
				cbInfo.JobID, err)
		}
	}

	return nil
}

// PreNotify runs the necessary actions before calling Notify
// and returns a callback info
func (n *Notifier) PreNotify(j *job.Job) (job.Callback, error) {
	j.CallbackCount++

	err := n.markCbInProgress(j)
	if err != nil {
		return job.Callback{}, err
	}

	cbInfo, err := j.CallbackInfo()
	if err != nil {
		return job.Callback{}, n.markCbFailed(j, err.Error())
	}

	return cbInfo, nil
}

// Notify posts callback info to job's destination by calling Notify
// on each backend.
func (n *Notifier) Notify(j *job.Job, cbInfo job.Callback) error {
	n.Log.Println("Performing callback action for", j, "...")

	cbType, cbDst := n.getCallbackTypeAndDst(j)

	b, ok := n.backends[cbType]
	if !ok {
		n.stats.Add(statsUndefinedBackendWithCallback, 1)
		err := fmt.Sprintf("Undefined backend %s for job %s", cbType, j)
		return n.retryOrFail(j, err)
	}

	err := b.Notify(cbDst, cbInfo)
	if err != nil {
		return n.retryOrFail(j, err.Error())
	}

	return nil
}

// retryOrFail checks the callback count of the current download
// and retries the callback if its Retry Counts < maxRetries else it marks
// it as failed
func (n *Notifier) retryOrFail(j *job.Job, err string) error {
	if j.CallbackCount >= maxCallbackRetries {
		return n.markCbFailed(j, err)
	}

	n.Log.Printf("Warn: Callback try no:%d failed for job:%s with: %s", j.CallbackCount, j, err)
	return n.Storage.QueuePendingCallback(j, time.Duration(j.CallbackCount)*RetryBackoffDuration)
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

// getCallbackTypeAndDst returns callback type and destination from either
// the job's callback_url or callback_type and callback_dst.
// When callback_url is present then as callback type the "http" is returned.
func (n *Notifier) getCallbackTypeAndDst(j *job.Job) (string, string) {
	if j.CallbackURL != "" {
		return "http", j.CallbackURL
	}

	return j.CallbackType, j.CallbackDst
}
