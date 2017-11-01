// Processor is one of the core entities of the downloader. It facilitates the
// processing of Jobs.
// Its main responsibility is to manage the creation and destruction of
// workerPools, which actually perform the Job download process.
//
// Each WorkerPool processes jobs belonging to a single aggregation and is in
// charge of imposing the corresponding rate-limit rules. Job routing for each
// Aggregation is performed through a redis list which is popped periodically
// by each WorkerPool. Popped jobs are then published to the WorkerPool's job
// channel. worker pools spawn worker goroutines (up to a max concurrency limit
// set for each aggregation) that consume from the aforementioned job channel
// and perform the actual download.
//
//   -----------------------------------------
//   |              Processor                |
//   |                                       |
//   |    ----------          ----------     |
//   |    |   WP   |          |   WP   |     |
//   |    |--------|          |--------|     |
//   |    |   W    |          |  W  W  |     |
//   |    | W   W  |          |  W  W  |     |
//   |    ----------          ----------     |
//   |                                       |
//   -----------------------------------------
//
// Cancellation and shutdown are coordinated through the use of contexts all
// along the stack.
// When a shutdown signal is received from the application it propagates from
// the processor to the active worker pools, stopping any in-progress jobs and
// gracefully shutting down the corresponding workers.
package processor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.skroutz.gr/skroutz/downloader/job"
	"golang.skroutz.gr/skroutz/downloader/storage"
)

// TODO: these should all be configuration options provided by the caller
const (
	workerMaxInactivity = 5 * time.Second
	backoffDuration     = 1 * time.Second
	maxDownloadRetries  = 3
)

type Processor struct {
	Storage *storage.Storage

	// ScanInterval is the amount of seconds to wait before re-scanning
	// Redis for new Aggregations.
	ScanInterval int

	// StorageDir is the filesystem location where the actual downloads
	// will be saved.
	StorageDir string

	// The client that will be used for the download requests
	Client *http.Client

	// The User-Agent to set in download requests
	UserAgent string

	Log *log.Logger

	// pools contain the existing worker pools
	pools map[string]*workerPool
}

// workerPool corresponds to an Aggregation. It spawns and instruments the
// workers that perform the actual downloads and enforces the rate-limit rules
// of the corresponding Aggregation.
type workerPool struct {
	aggr             job.Aggregation
	p                *Processor
	numActiveWorkers int32
	log              *log.Logger

	// jobChan is the channel that distributes jobs to the respective
	// workers
	jobChan chan job.Job
}

// New initializes and returns a Processor, or an error if storageDir
// is not writable.
//
// TODO: only add REQUIRED arguments, the rest should be set from the struct
func New(storage *storage.Storage, scanInterval int, storageDir string, client *http.Client, logger *log.Logger) (Processor, error) {
	// verify we can write to storageDir
	//
	// TODO: create an error type to wrap these errors
	tmpf, err := ioutil.TempFile(storageDir, "write-check-")
	if err != nil {
		return Processor{}, errors.New("Error verifying storage directory is writable: " + err.Error())
	}
	_, err = tmpf.Write([]byte("a"))
	if err != nil {
		tmpf.Close()
		os.Remove(tmpf.Name())
		return Processor{}, errors.New("Error verifying storage directory is writable: " + err.Error())
	}
	err = tmpf.Close()
	if err != nil {
		return Processor{}, errors.New("Error verifying storage directory is writable: " + err.Error())
	}
	err = os.Remove(tmpf.Name())
	if err != nil {
		return Processor{}, errors.New("Error verifying storage directory is writable: " + err.Error())
	}

	return Processor{
		Storage:      storage,
		StorageDir:   storageDir,
		ScanInterval: scanInterval,
		Client:       client,
		Log:          logger,
		pools:        make(map[string]*workerPool)}, nil
}

// Start starts p.
//
// It scans Redis for new Aggregations and spawns the corresponding worker
// pools when needed.
func (p *Processor) Start(closeCh chan struct{}) {
	p.Log.Println("Starting...")
	workerClose := make(chan string)
	var wpWg sync.WaitGroup
	scanTicker := time.NewTicker(time.Duration(p.ScanInterval) * time.Second)
	defer scanTicker.Stop()
	ctx, cancel := context.WithCancel(context.Background())

	p.collectRogueDownloads()

PROCESSOR_LOOP:
	for {
		select {
		// An Aggregation worker pool closed due to inactivity
		case aggrID := <-workerClose:
			delete(p.pools, aggrID)
		// Close signal from upper layer
		case <-closeCh:
			cancel()
			break PROCESSOR_LOOP
		case <-scanTicker.C:
			var cursor uint64
			for {
				var keys []string
				var err error
				keys, cursor, err = p.Storage.Redis.Scan(cursor, storage.JobsKeyPrefix+"*", 50).Result()
				if err != nil {
					p.Log.Println(fmt.Errorf("Error scanning keys: %v", err))
					break
				}

				for _, ag := range keys {
					aggrID := strings.TrimPrefix(ag, storage.JobsKeyPrefix)
					if _, ok := p.pools[aggrID]; !ok {
						aggr, err := p.Storage.GetAggregation(aggrID)
						if err != nil {
							p.Log.Printf("Error fetching aggregation with id '%s': %s", aggrID, err)
							continue
						}
						wp := p.newWorkerPool(aggr)
						p.pools[aggrID] = &wp
						wpWg.Add(1)

						go func() {
							defer wpWg.Done()
							wp.start(ctx, p.StorageDir)
							// The processor only needs to be informed about non-forced close ( without context-cancel )
							if ctx.Err() == nil {
								workerClose <- wp.aggr.ID
							}
						}()
					}
				}

				if cursor == 0 {
					break
				}
			}
		}
	}

	wpWg.Wait()
	p.Log.Println("Shutting down...")
	closeCh <- struct{}{}
}

func (p *Processor) storageFile(j *job.Job) (*os.File, error) {
	jobPath := path.Join(p.StorageDir, j.Path())

	err := os.MkdirAll(filepath.Dir(jobPath), os.FileMode(0755))
	if err != nil {
		return nil, err
	}

	return os.Create(jobPath)
}

// collectRogueDownloads Scans Redis for jobs that have InProgress DownloadState.
// This indicates they are leftover from an interrupted previous run and should get requeued.
func (p *Processor) collectRogueDownloads() {
	var cursor uint64
	var rogueCount uint64

	for {
		var keys []string
		var err error
		keys, cursor, err = p.Storage.Redis.Scan(cursor, storage.JobKeyPrefix+"*", 50).Result()
		if err != nil {
			p.Log.Println("Error scanning Redis for rogue downloads:", err)
			break
		}

		for _, jobID := range keys {
			strCmd := p.Storage.Redis.HGet(jobID, "DownloadState")
			if strCmd.Err() != nil {
				p.Log.Println(strCmd.Err())
				continue
			}
			if job.State(strCmd.Val()) == job.StateInProgress {
				jb, err := p.Storage.GetJob(strings.TrimPrefix(jobID, storage.JobKeyPrefix))
				if err != nil {
					p.Log.Printf("Error fetching job with id '%s' from Redis: %s", jobID, err)
					continue
				}
				err = p.Storage.QueuePendingDownload(&jb)
				if err != nil {
					p.Log.Printf("Error queueing job with id '%s' for download: %s", jb.ID, err)
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
		p.Log.Printf("Queued %d rogue downloads", rogueCount)
	}

}

// newWorkerPool initializes and returns a WorkerPool for aggr.
func (p *Processor) newWorkerPool(aggr job.Aggregation) workerPool {
	logPrefix := fmt.Sprintf("%s[worker pool:%s] ", p.Log.Prefix(), aggr.ID)

	return workerPool{
		aggr:    aggr,
		jobChan: make(chan job.Job),
		p:       p,
		log:     log.New(os.Stderr, logPrefix, log.Ldate|log.Ltime)}
}

// increaseWorkers atomically increases the activeWorkers counter of wp by 1
func (wp *workerPool) increaseWorkers() {
	atomic.AddInt32(&wp.numActiveWorkers, 1)
}

// decreaseWorkers atomically decreases the activeWorkers counter of wp by 1
func (wp *workerPool) decreaseWorkers() {
	atomic.AddInt32(&wp.numActiveWorkers, -1)
}

// activeWorkers return the number of existing active workers in wp.
func (wp *workerPool) activeWorkers() int {
	return int(atomic.LoadInt32(&wp.numActiveWorkers))
}

// start starts wp. It is the core WorkerPool work loop. It can be stopped by
// using ctx.
//
// All worker instrumentation, job popping from Redis and shutdown logic is
// performed in start.
func (wp *workerPool) start(ctx context.Context, storageDir string) {
	wp.log.Printf("Started working...")

	var wg sync.WaitGroup

WORKERPOOL_LOOP:
	for {
		select {
		case <-ctx.Done():
			wp.log.Printf("Received shutdown signal...")
			break WORKERPOOL_LOOP
		default:
			job, err := wp.p.Storage.PopJob(&wp.aggr)
			if err != nil {
				switch err {
				case storage.ErrEmptyQueue:
					// Stop the WorkerPool if
					// 1) The queue is empty
					// 2) No workers are running
					if wp.activeWorkers() == 0 {
						wp.log.Println("Closing due to inactivity...")
						break WORKERPOOL_LOOP
					}
				case storage.ErrRetryLater:
					// noop
				default:
					wp.log.Println("Error popping job from Redis:", err)
				}

				// backoff & wait for workers to finish or a job to be queued
				time.Sleep(backoffDuration)
				continue
			}
			if wp.activeWorkers() < wp.aggr.Limit {
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer wp.decreaseWorkers()
					wp.increaseWorkers()
					wp.work(ctx, storageDir)
				}()
			}
			wp.jobChan <- job
		}
	}

	close(wp.jobChan)
	wg.Wait()
	wp.log.Printf("Bye!")
}

// work consumes Jobs from wp and performs them.
func (wp *workerPool) work(ctx context.Context, saveDir string) {
	lastActive := time.Now()

	for {
		select {
		case job, ok := <-wp.jobChan:
			if !ok {
				return
			}

			wp.perform(ctx, &job)
			lastActive = time.Now()
		default:
			if time.Since(lastActive) > workerMaxInactivity {
				return
			}

			// No job found, backing off
			time.Sleep(backoffDuration)
		}
	}
}

// perform downloads the resource denoted by j.URL and updates its state in
// Redis accordingly. It may retry downloading on certain errors.
func (wp *workerPool) perform(ctx context.Context, j *job.Job) {
	err := wp.markJobInProgress(j)
	if err != nil {
		wp.log.Printf("perform: Error marking %s as in-progress: %s", j, err)
		return
	}

	req, err := http.NewRequest("GET", j.URL, nil)
	if err != nil {
		wp.log.Printf("perform: Error initializing download request for %s: %s", j, err)
		err = wp.markJobFailed(j, fmt.Sprintf("Could not initialize request: %s", err))
		if err != nil {
			wp.log.Printf("perform: Error marking %s as failed: %s", j, err)
		}
		return
	}
	if wp.p.UserAgent != "" {
		req.Header.Set("User-Agent", wp.p.UserAgent)
	}

	j.DownloadCount++
	wp.log.Println("Performing request for", j, "...")
	resp, err := wp.p.Client.Do(req.WithContext(ctx))
	if err != nil {
		if strings.Contains(err.Error(), "x509") || strings.Contains(err.Error(), "tls") {
			err = wp.markJobFailed(j, fmt.Sprintf("TLS Error occured: %s", err))
			if err != nil {
				wp.log.Printf("perform: Error marking %s failed: %s", j, err)
			}
			return
		}
		err = wp.requeueOrFail(j, err.Error())
		if err != nil {
			wp.log.Printf("perform: Error requeueing %s: %s", j, err)
		}
		return
	}

	if resp.StatusCode >= http.StatusInternalServerError {
		err = wp.requeueOrFail(j, fmt.Sprintf("Received status code %s", resp.Status))
		if err != nil {
			wp.log.Printf("perform: Error requeueing %s: %s", j, err)
		}
		return
	} else if resp.StatusCode >= http.StatusBadRequest {
		err = wp.markJobFailed(j, fmt.Sprintf("Received status code %d", resp.StatusCode))
		if err != nil {
			wp.log.Printf("perform: Error marking %s failed: %s", j, err)
		}
		return
	}
	defer resp.Body.Close()

	out, err := wp.p.storageFile(j)
	if err != nil {
		wp.log.Printf("perform: Error creating download file for %s: %s", j, err)
		err = wp.requeueOrFail(j, fmt.Sprintf("Could not create/write to file, %v", err))
		if err != nil {
			wp.log.Printf("perform: Error requeueing %s: %s", j, err)
		}
		return
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		wp.log.Printf("perform: Error writing to download file for %s: %s", j, err)
		err = wp.requeueOrFail(j, fmt.Sprintf("Error writing to download file: %s", err))
		if err != nil {
			wp.log.Printf("perform: Error requeueing %s: %s", j, err)
		}
		return
	}

	err = out.Sync()
	if err != nil {
		wp.log.Printf("perform: Error syncing download file for %s: %s", j, err)
	}

	err = wp.markJobSuccess(j)
	if err != nil {
		wp.log.Printf("perform: Error marking %s successful: %s", j, err)
	}
}

// requeueOrFail checks the retry count of the current download
// and retries the job if its RetryCount < maxRetries else it marks
// it as failed
func (wp *workerPool) requeueOrFail(j *job.Job, meta string) error {
	if j.DownloadCount >= maxDownloadRetries {
		return wp.markJobFailed(j, meta)
	}
	return wp.p.Storage.QueuePendingDownload(j)
}

func (wp *workerPool) markJobInProgress(j *job.Job) error {
	j.DownloadState = job.StateInProgress
	j.DownloadMeta = ""
	return wp.p.Storage.SaveJob(j)
}

// Marks j as successful and enqueues it for callback
func (wp *workerPool) markJobSuccess(j *job.Job) error {
	j.DownloadState = job.StateSuccess
	j.DownloadMeta = ""

	// NOTE: we depend on QueuePendingCallback calling SaveJob(j)
	return wp.p.Storage.QueuePendingCallback(j)
}

// Marks j as failed and enqueues it for callback
func (wp *workerPool) markJobFailed(j *job.Job, meta ...string) error {
	j.DownloadState = job.StateFailed
	j.DownloadMeta = strings.Join(meta, "\n")

	// NOTE: we depend on QueuePendingCallback calling SaveJob(j)
	return wp.p.Storage.QueuePendingCallback(j)
}
