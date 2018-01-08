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
	"crypto/tls"
	"errors"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.skroutz.gr/skroutz/downloader/job"
	"golang.skroutz.gr/skroutz/downloader/processor/diskcheck"
	"golang.skroutz.gr/skroutz/downloader/processor/mimetype"
	"golang.skroutz.gr/skroutz/downloader/stats"
	"golang.skroutz.gr/skroutz/downloader/storage"
)

var (
	RetryBackoffDuration = 2 * time.Minute
	newChecker           = diskcheck.New

	// Based on http.DefaultTransport
	//
	// See https://golang.org/pkg/net/http/#RoundTripper
	httpTransport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second, // was 30 * time.Second
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   4 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		// Bypass tls errors with a few upstream servers (local error: tls: no renegotiation)
		// We will allow a single server-initiated renegotiation attempt
		// See:
		// https://golang.org/pkg/crypto/tls/#RenegotiationSupport
		// https://github.com/golang/go/issues/5742
		TLSClientConfig: &tls.Config{Renegotiation: tls.RenegotiateOnceAsClient},
	}
)

// TODO: these should all be configuration options provided by the caller
const (
	workerMaxInactivity = 5 * time.Second
	backoffDuration     = 1 * time.Second
	maxDownloadRetries  = 3

	//Metric Identifiers
	statsMaxWorkers                = "maxWorkers"                //Gauge
	statsMaxWorkerPools            = "maxWorkerPools"            //Gauge
	statsWorkers                   = "workers"                   //Gauge
	statsWorkerPools               = "workerPools"               //Gauge
	statsSpawnedWorkerPools        = "spawnedWorkerPools"        //Counter
	statsSpawnedWorkers            = "spawnedWorkers"            //Counter
	statsFailures                  = "failures"                  //Counter
	statsResponseCodePrefix        = "download.response."        //Counter
	statsReaperFailures            = "reaperFailures"            //Counter
	statsReaperSuccessfulDeletions = "reaperSuccessfulDeletions" //Counter

	// diskChecker settings
	diskHigh     = 95
	diskLow      = 90
	diskInterval = 1 * time.Minute
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

	// Interval between each stats flush
	StatsIntvl time.Duration

	// pools contain the existing worker pools
	pools map[string]*workerPool

	stats *stats.Stats
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

func init() {
	// Indicates we are in test mode
	if _, testMode := os.LookupEnv("DOWNLOADER_TEST_TIME"); testMode {
		RetryBackoffDuration = 200 * time.Millisecond
	}
}

// New initializes and returns a Processor, or an error if storageDir
// is not writable.
//
// TODO: only add REQUIRED arguments, the rest should be set from the struct
func New(storage *storage.Storage, scanInterval int, storageDir string, logger *log.Logger) (Processor, error) {
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

	client := &http.Client{
		Transport: httpTransport,
		Timeout:   10 * time.Second, // Larger than Dial + TLS timeout
	}

	return Processor{
		Storage:      storage,
		StorageDir:   storageDir,
		ScanInterval: scanInterval,
		StatsIntvl:   5 * time.Second,
		Client:       client,
		Log:          logger,
		pools:        make(map[string]*workerPool),
		stats:        stats.New("Processor", time.Second, func(m *expvar.Map) {}),
	}, nil
}

// Start starts p.
//
// It spawns helpers goroutines & starts spawning worker pools by scanning Redis for new Aggregations
func (p *Processor) Start(closeCh chan struct{}) {
	p.Log.Println("Starting...")
	p.collectRogueDownloads()

	ctx, cancel := context.WithCancel(context.TODO())

	var processorWg sync.WaitGroup
	processorWg.Add(1)
	go func() {
		defer processorWg.Done()
		p.reaper(ctx)
	}()

	p.stats = stats.New("Processor", p.StatsIntvl,
		func(m *expvar.Map) {
			err := p.Storage.SetStats("processor", m.String(), 2*p.StatsIntvl) // Autoremove stats after 2 times the interval
			if err != nil {
				p.Log.Println("Could not report stats", err)
			}
		})
	go p.stats.Run(ctx)

	var diskChecker diskcheck.Checker
	diskChecker, err := newChecker(p.StorageDir, diskHigh, diskLow, diskInterval)
	if err != nil {
		p.Log.Println("Error initializing disk checker: ", err)
	}
	processorWg.Add(1)
	go func() {
		defer processorWg.Done()
		diskChecker.Run(ctx)
	}()

	// Spawn worker pools loop with a seperate context
	// so we can stop it indepentetly.
	var loopWg sync.WaitGroup
	loopCtx, loopCancel := context.WithCancel(context.TODO())
	spawnLoop := func() {
		defer loopWg.Done()
		p.spawnPools(loopCtx)
	}
	loopWg.Add(1)
	go spawnLoop()

PROCESSOR_LOOP:
	for {
		select {
		case health := <-diskChecker.C():
			if health == diskcheck.Sick {
				p.Log.Println("Sick disk, stopping the worker pool loop...")
				loopCancel()
				loopWg.Wait()
			} else {
				p.Log.Println("Healthy disk, starting the worker pool loop...")
				loopCtx, loopCancel = context.WithCancel(context.TODO())
				loopWg.Add(1)
				go spawnLoop()
			}
		case <-closeCh:
			loopCancel()
			cancel()
			break PROCESSOR_LOOP
		}
	}

	p.Log.Println("Shutting down...")
	loopWg.Wait()
	processorWg.Wait()
	closeCh <- struct{}{}
}

// spawnPools spawns & monitors worker pools. When ctx is done, it forcibly stops all workers,
// cleanups the pools map & waits for all goroutines to finish.
func (p *Processor) spawnPools(ctx context.Context) {
	workerClose := make(chan string)
	var poolWg sync.WaitGroup
	scanTicker := time.NewTicker(time.Duration(p.ScanInterval) * time.Second)
	defer scanTicker.Stop()

	maxWorkerPools := new(expvar.Int)

POOLS_LOOP:
	for {
		select {
		// An Aggregation worker pool closed due to inactivity
		case aggrID := <-workerClose:
			delete(p.pools, aggrID)
			p.stats.Add(statsWorkerPools, -1)
		// Close signal from upper layer
		//
		// Note that we don't have to explicitly cancel the spawned worker pools
		// since they share the same context.
		case <-ctx.Done():
			break POOLS_LOOP
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
							p.Log.Printf("Error fetching aggregation with id '%s': %s",
								aggrID, err)
							if err != storage.ErrNotFound {
								continue
							}
							p.Log.Printf("Using aggregation with id '%s', and limit: %d",
								aggr.ID, aggr.Limit)
						}
						wp := p.newWorkerPool(aggr)
						p.pools[aggrID] = &wp

						//Report Metrics
						p.stats.Add(statsWorkerPools, 1)
						p.stats.Add(statsSpawnedWorkerPools, 1)
						if pools := int64(len(p.pools)); maxWorkerPools.Value() < pools {
							maxWorkerPools.Set(pools)
							p.stats.Set(statsMaxWorkerPools, maxWorkerPools)
						}

						poolWg.Add(1)
						go func() {
							defer poolWg.Done()
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

	poolWg.Wait()
	// All worker pools have stopped, it's safe to empty the pool.
	for k := range p.pools {
		delete(p.pools, k)
	}
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
				err = p.Storage.QueuePendingDownload(&jb, 0)
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
		log:     log.New(os.Stderr, logPrefix, log.Ldate|log.Ltime),
	}
}

// increaseWorkers atomically increases the activeWorkers counter of wp by 1
func (wp *workerPool) increaseWorkers() {
	atomic.AddInt32(&wp.numActiveWorkers, 1)

	// Update stats
	wp.p.stats.Add(statsSpawnedWorkers, 1)
	wp.p.stats.Add(statsWorkers, 1)

	//update max workers
	activeWorkers, ok := wp.p.stats.Get(statsWorkers).(*expvar.Int)
	if !ok {
		wp.p.Log.Println("Could not get active workers from stats")
		return
	}

	max, ok := wp.p.stats.Get(statsMaxWorkers).(*expvar.Int)
	if ok && max.Value() >= activeWorkers.Value() {
		return
	}

	max = new(expvar.Int)
	max.Set(activeWorkers.Value())
	wp.p.stats.Set(statsMaxWorkers, max)
}

// decreaseWorkers atomically decreases the activeWorkers counter of wp by 1
func (wp *workerPool) decreaseWorkers() {
	wp.p.stats.Add(statsWorkers, -1)
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
	startedAt := time.Now()
	// Track the number of processed downloads.
	downloads := 0

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
			downloads++
		}
	}

	err := wp.p.Storage.RemoveAggregation(wp.aggr.ID)
	if err != nil {
		wp.log.Printf("Error removing aggregation: %s", err)
	}

	close(wp.jobChan)
	wg.Wait()

	lifetime := time.Now().Sub(startedAt).Truncate(time.Second)
	wp.log.Printf("Bye! (lifetime:%s,downloads:%d)", lifetime, downloads)
}

// work consumes Jobs from wp and performs them.
func (wp *workerPool) work(ctx context.Context, saveDir string) {
	lastActive := time.Now()

	//initialize a validator to be used by the current worker
	validator, err := mimetype.New()
	if err != nil {
		wp.log.Println("Error: Could not create new validator", err)
		return
	}

	defer validator.Close()

	for {
		select {
		case job, ok := <-wp.jobChan:
			if !ok {
				return
			}

			wp.perform(ctx, &job, validator)
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
func (wp *workerPool) perform(ctx context.Context, j *job.Job, validator *mimetype.Validator) {
	err := wp.markJobInProgress(j)
	if err != nil {
		wp.log.Printf("perform: Error marking %s as in-progress: %s", j, err)
		return
	}

	req, err := http.NewRequest("GET", j.URL, nil)
	if err != nil {
		wp.log.Printf("perform: Error initializing download request for %s: %s", j, err)
		wp.p.stats.Add(statsFailures, 1)
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
		wp.p.Log.Printf("perform: Error downloading job %s: %s", j, err)
		if strings.Contains(err.Error(), "x509") || strings.Contains(err.Error(), "tls") {
			err = wp.markJobFailed(j, fmt.Sprintf("TLS Error occured: %s", err))
			wp.p.stats.Add(fmt.Sprintf("%s%s", statsResponseCodePrefix, "tls"), 1)
			if err != nil {
				wp.log.Printf("perform: Error marking %s failed: %s", j, err)
			}
			return
		}

		if err == context.Canceled {
			// Do not count canceled download towards MaxRetries
			j.DownloadCount--
		}
		wp.p.stats.Add(fmt.Sprintf("%s%s", statsResponseCodePrefix, "other"), 1)
		err = wp.requeueOrFail(j, err.Error())
		if err != nil {
			wp.log.Printf("perform: Error requeueing %s: %s", j, err)
		}
		return
	}

	j.ResponseCode = resp.StatusCode
	wp.p.Log.Printf("Received status code %d for job: %s", resp.StatusCode, j)
	wp.p.stats.Add(fmt.Sprintf("%s%d", statsResponseCodePrefix, resp.StatusCode), 1)

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
		wp.p.stats.Add(statsFailures, 1)
		err = wp.requeueOrFail(j, fmt.Sprintf("Could not create/write to file, %v", err))
		if err != nil {
			wp.log.Printf("perform: Error requeueing %s: %s", j, err)
		}
		return
	}
	defer out.Close()

	if j.MimeType != "" {
		if validator == nil {
			wp.p.Log.Printf("Error: No available mime type validator, %s", j)
			// This is problably an error on our side so do not bump the download count
			j.DownloadCount--
			wp.requeueOrFail(j, "MimeType validator: nil")
			return
		}
		validator.Reset(j.MimeType)

		// Use TeeReader to copy reads to the output file
		err = validator.Read(io.TeeReader(resp.Body, out))
		if err != nil {
			if _, ok := err.(mimetype.ErrMimeTypeMismatch); ok {
				wp.p.stats.Add(fmt.Sprintf("%s%s", statsResponseCodePrefix, "mime"), 1)
				wp.p.Log.Printf("perform: Error validationg mime type for %s: %s", j, err)
				err = wp.markJobFailed(j, err.Error())
			} else {
				wp.p.stats.Add(fmt.Sprintf("%s%s", statsResponseCodePrefix, "body"), 1)
				err = wp.requeueOrFail(j, err.Error())
			}

			if err != nil {
				wp.log.Printf("perform: storage error during for %s: %s", j, err)
			}
			return
		}
	}

	_, err = io.Copy(out, resp.Body)
	if err != nil {

		if err == context.Canceled {
			// Do not count canceled download towards MaxRetries
			j.DownloadCount--
		} else {
			wp.p.stats.Add(fmt.Sprintf("%s%s", statsResponseCodePrefix, "body"), 1)
		}

		wp.log.Printf("perform: Error downloading file for %s: %s", j, err)
		err = wp.requeueOrFail(j, fmt.Sprintf("Error downloading file for %s: %s", j, err))
		if err != nil {
			wp.log.Printf("perform: Error requeueing %s: %s", j, err)
		}
		return
	}

	err = out.Sync()
	if err != nil {
		wp.log.Printf("perform: Error syncing download file for %s: %s", j, err)
		wp.p.stats.Add(statsFailures, 1)
		return
	}

	err = wp.markJobSuccess(j)
	if err != nil {
		wp.log.Printf("perform: Error marking %s successful: %s", j, err)
	}
}

// requeueOrFail checks the retry count of the current download
// and retries the job if its RetryCount < maxRetries else it marks
// it as failed
func (wp *workerPool) requeueOrFail(j *job.Job, err string) error {
	if j.DownloadCount >= maxDownloadRetries {
		return wp.markJobFailed(j, err)
	}
	return wp.p.Storage.QueuePendingDownload(j, time.Duration(j.DownloadCount)*RetryBackoffDuration)
}

func (wp *workerPool) markJobInProgress(j *job.Job) error {
	j.DownloadState = job.StateInProgress
	j.DownloadMeta = ""
	j.ResponseCode = 0
	return wp.p.Storage.SaveJob(j)
}

// Marks j as successful and enqueues it for callback
func (wp *workerPool) markJobSuccess(j *job.Job) error {
	j.DownloadState = job.StateSuccess
	j.DownloadMeta = ""

	// NOTE: we depend on QueuePendingCallback calling SaveJob(j)
	return wp.p.Storage.QueuePendingCallback(j, 0)
}

// Marks j as failed and enqueues it for callback
func (wp *workerPool) markJobFailed(j *job.Job, meta ...string) error {
	j.DownloadState = job.StateFailed
	j.DownloadMeta = strings.Join(meta, "\n")

	// NOTE: we depend on QueuePendingCallback calling SaveJob(j)
	return wp.p.Storage.QueuePendingCallback(j, 0)
}

// reaper is responsible of deleting jobs ( along with their associated files ) that have
// been reported as not needed any more.
// It consumes using PopRip and acts on the provided job ids.
func (p *Processor) reaper(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			p.Log.Println("Reaper Exiting... Bye!")
			return
		default:
			j, err := p.Storage.PopRip()
			if err != nil {
				if err == storage.ErrEmptyQueue {
					time.Sleep(backoffDuration)
				} else {
					p.Log.Println("Error popping job from RipQueue", err)
				}
				continue
			}

			filePath := path.Join(p.StorageDir, j.Path())
			p.Log.Printf("reaper: Deleting file [%s] for job %s", filePath, j)
			err = os.Remove(filePath)
			if err != nil && !os.IsNotExist(err) {
				p.Log.Printf("Error: Could not delete [%s] for job: %s, %s", filePath, j, err.Error())
				p.stats.Add(statsReaperFailures, 1)
				continue
			}

			err = p.Storage.RemoveJob(j.ID)
			if err != nil {
				p.Log.Println(fmt.Sprintf("Error deleting job %s, %s", j, err.Error()))
				p.stats.Add(statsReaperFailures, 1)
				continue
			}

			p.stats.Add(statsReaperSuccessfulDeletions, 1)
		}
	}
}
