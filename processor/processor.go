// Package processor is one of the core entities of the downloader. It facilitates the
// processing of Jobs. Its main responsibility is to manage the creation and destruction of
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

	"github.com/skroutz/downloader/job"
	"github.com/skroutz/downloader/processor/diskcheck"
	derrors "github.com/skroutz/downloader/processor/errors"
	"github.com/skroutz/downloader/processor/mimetype"
	"github.com/skroutz/downloader/stats"
	"github.com/skroutz/downloader/storage"
)

var (
	// RetryBackoffDuration indicates the time to wait between retries.
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

// Processor is the main entity of the downloader.
// For more info of its architecture see package level doc.
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

func (p *Processor) storagePath(j *job.Job) string {
	return path.Join(p.StorageDir, j.Path())
}

const tmpFileExt = ".tmp"

func (p *Processor) tmpStoragePath(j *job.Job) string {
	return path.Join(p.StorageDir, j.ID+tmpFileExt)
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

func (wp *workerPool) download(ctx context.Context, j *job.Job, validator *mimetype.Validator) derrors.DownloadError {
	req, err := http.NewRequest("GET", j.URL, nil)
	if err != nil {
		// This actually indicates a malformed url
		return derrors.E("creating request", err)
	}
	if wp.p.UserAgent != "" {
		req.Header.Set("User-Agent", wp.p.UserAgent)
	}

	resp, err := wp.p.Client.Do(req.WithContext(ctx))
	if err != nil {
		if strings.Contains(err.Error(), "x509") || strings.Contains(err.Error(), "tls") {
			wp.p.stats.Add(fmt.Sprintf("%s%s", statsResponseCodePrefix, "tls"), 1)
			return derrors.Errorf("performing request", "TLS Error occured: %s", err)
		}
		return derrors.E("performing request", err).Retriable()
	}
	defer resp.Body.Close()

	j.ResponseCode = resp.StatusCode
	wp.p.stats.Add(fmt.Sprintf("%s%d", statsResponseCodePrefix, resp.StatusCode), 1)

	if resp.StatusCode >= http.StatusInternalServerError {
		return derrors.Errorf("processing response", "Received status code %s", resp.Status).Retriable()
	} else if resp.StatusCode >= http.StatusBadRequest {
		return derrors.Errorf("processing response", "Received status code %s", resp.Status)
	}

	out, err := os.Create(wp.p.tmpStoragePath(j))
	if err != nil {
		return derrors.E("creating tmp file", err).Internal().Retriable()
	}
	defer out.Close()

	if j.MimeType != "" {
		if validator == nil {
			panic("No available mime type validator")
		}

		validator.Reset(j.MimeType)
		if err = validator.Read(io.TeeReader(resp.Body, out)); err != nil {
			if _, ok := err.(mimetype.ErrMimeTypeMismatch); ok {
				wp.p.stats.Add(fmt.Sprintf("%s%s", statsResponseCodePrefix, "mime"), 1)
				return derrors.E("validating mime type", err)
			}
			wp.p.stats.Add(fmt.Sprintf("%s%s", statsResponseCodePrefix, "body"), 1)
			return derrors.E("downloading file", err).Retriable()
		}
	}

	if _, err = io.Copy(out, resp.Body); err != nil {
		wp.p.stats.Add(fmt.Sprintf("%s%s", statsResponseCodePrefix, "body"), 1)
		return derrors.E("downloading file", err).Retriable()
	}

	if err = out.Sync(); err != nil {
		return derrors.E("syncing to disk", err).Internal().Retriable()
	}

	path := wp.p.storagePath(j)
	if err = os.MkdirAll(filepath.Dir(path), os.FileMode(0755)); err != nil {
		return derrors.E("creating download directory", err).Internal().Retriable()
	}

	if err = os.Rename(out.Name(), path); err != nil {
		return derrors.E("moving file to perm location", err).Internal().Retriable()
	}

	return nil
}

// perform downloads the resource denoted by j.URL and updates its state in
// Redis accordingly. It may retry downloading on certain errors.
func (wp *workerPool) perform(ctx context.Context, j *job.Job, validator *mimetype.Validator) {
	var err error
	if err = wp.markJobInProgress(j); err != nil {
		wp.log.Printf("perform: Error marking %s as in-progress: %s", j, err)
		return
	}
	j.DownloadCount++
	wp.log.Println("perform: Starting download for", j, "...")

	if de := wp.download(ctx, j, validator); de != nil {
		wp.log.Println("perform: Download Failed for", j, de)

		// Do not mark this as a download try if the error is on our side,
		// or the request context was cancelled
		if de.Err() == context.Canceled || de.IsInternal() {
			j.DownloadCount--
		}

		if de.IsInternal() {
			wp.p.stats.Add(statsFailures, 1)
		}

		if de.IsRetriable() {
			if err = wp.requeueOrFail(j, de.Error()); err != nil {
				wp.log.Printf("perform: Error requeing %s : %s", j, err)
			}
		} else {
			if err = wp.markJobFailed(j, de.Error()); err != nil {
				wp.log.Printf("perform: Error marking %s Failed: %s", j, err)
			}
		}

		if err := os.Remove(wp.p.tmpStoragePath(j)); err != nil && !os.IsNotExist(err) {
			wp.log.Printf("perform: Error deleting temp file %s, %s, %s", wp.p.tmpStoragePath(j), j, err)
		}
		return
	}
	wp.log.Println("perform: Successfully completed download for", j)

	if err = wp.markJobSuccess(j); err != nil {
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
				if err == storage.ErrEmptyQueue || err == storage.ErrRetryLater {
					time.Sleep(backoffDuration)
				} else {
					p.Log.Println("Error popping job from RipQueue", err)
				}
				continue
			}

			filePath := path.Join(p.StorageDir, j.Path())
			err = os.Remove(filePath)
			if err != nil && !os.IsNotExist(err) {
				p.Log.Printf("Error: Could not delete [%s] for job: %s, %s", filePath, j, err.Error())
				p.stats.Add(statsReaperFailures, 1)
				continue
			}

			if err == nil {
				p.Log.Printf("reaper: Deleted file [%s] for job %s", filePath, j)
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
