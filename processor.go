// processor is the main entity of the downloader.
//
// It is a service, facilitating the processing of Jobs.
// Its main responsibility is to manage the creation and destruction of
// WorkerPools, which actually perform the Job download process.
//
// Each WorkerPool processes jobs belonging to a single aggregation and is in
// charge of imposing the corresponding rate-limit rules. Job routing for each
// Aggregation is performed through a redis list which is popped periodically
// by each WorkerPool. Popped jobs are then published to the WorkerPool's job
// channel. WorkerPools spawn worker goroutines (up to a max concurrency limit
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
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var client *http.Client

func init() {
	client = &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{}},
		Timeout:   time.Duration(3) * time.Second}
}

// Processor is in charge of scanning Redis for new aggregations and initiating
// the corresponding WorkerPools when needed. It also manages and instruments
// the WorkerPools.
type Processor struct {
	// storageDir is the filesystem location where the actual downloads
	// will be saved.
	storageDir string

	// scanInterval is the amount of seconds to wait before re-scanning
	// Redis for new Aggregations.
	scanInterval int

	// pools contain the existing WorkerPools
	pools map[string]*WorkerPool

	Log *log.Logger
}

// WorkerPool corresponds to an Aggregation. It spawns and instruments the
// workers that perform the actual downloads and enforces the rate-limit rules
// of the corresponding Aggregation.
type WorkerPool struct {
	Aggr          Aggregation
	activeWorkers int32
	shutdown      chan struct{}
	log           *log.Logger

	// jobChan is the channel that distributes jobs to the respective
	// workers
	jobChan chan Job
}

// NewWorkerPool initializes and returns a WorkerPool for aggr.
func NewWorkerPool(aggr Aggregation) WorkerPool {
	logPrefix := fmt.Sprintf("[Processor][WorkerPool:%s] ", aggr.ID)

	return WorkerPool{
		Aggr:    aggr,
		jobChan: make(chan Job),
		log:     log.New(os.Stderr, logPrefix, log.Ldate|log.Ltime)}
}

// increaseWorkers atomically increases the activeWorkers counter of wp by 1
func (wp *WorkerPool) increaseWorkers() {
	atomic.AddInt32(&wp.activeWorkers, 1)
}

// decreaseWorkers atomically decreases the activeWorkers counter of wp by 1
func (wp *WorkerPool) decreaseWorkers() {
	atomic.AddInt32(&wp.activeWorkers, -1)
}

// ActiveWorkers return the number of existing active workers in wp.
func (wp *WorkerPool) ActiveWorkers() int {
	return int(atomic.LoadInt32(&wp.activeWorkers))
}

// Start starts wp. It is the core WorkerPool work loop. It can be stopped by
// using ctx.
//
// All worker instrumentation, job popping from Redis and shutdown logic is
// performed in Start.
func (wp *WorkerPool) Start(ctx context.Context, storageDir string) {
	wp.log.Printf("Started working...")

	var wg sync.WaitGroup

WORKERPOOL_LOOP:
	for {
		select {
		case <-ctx.Done():
			wp.log.Printf("Received shutdown signal...")
			close(wp.jobChan)
			break WORKERPOOL_LOOP
		default:
			job, err := wp.Aggr.PopJob()
			if err != nil {
				if err.Error() != "Queue is empty" {
					log.Println(err)
				}
				continue
			}
			if wp.ActiveWorkers() < wp.Aggr.Limit {
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

	wg.Wait()
	wp.log.Printf("Closing...")
}

// work consumes Jobs from wp and performs them.
func (wp *WorkerPool) work(ctx context.Context, saveDir string) {
	for {
		select {
		case job := <-wp.jobChan:
			job.Perform(ctx, saveDir)
		default:
			return
		}
	}
}

// NewProcessor initializes and returns a Processor, or an error if storageDir
// is not writable.
func NewProcessor(scaninter int, storageDir string) (Processor, error) {
	// verify we can write to storageDir
	tmpf, err := ioutil.TempFile(storageDir, "downloader-")
	if err != nil {
		return Processor{}, err
	}
	_, err = tmpf.Write([]byte("a"))
	if err != nil {
		tmpf.Close()
		os.Remove(tmpf.Name())
		return Processor{}, err
	}
	err = tmpf.Close()
	if err != nil {
		return Processor{}, err
	}
	err = os.Remove(tmpf.Name())
	if err != nil {
		return Processor{}, err
	}

	return Processor{
		storageDir:   storageDir,
		scanInterval: scaninter,
		pools:        make(map[string]*WorkerPool),
		Log:          log.New(os.Stderr, "[Processor] ", log.Ldate|log.Ltime)}, nil
}

// Start starts p.
//
// It scans Redis for new Aggregations and spawns the corresponding worker
// pools when needed.
func (p *Processor) Start(closeCh chan struct{}) {
	p.Log.Println("Starting...")
	workerClose := make(chan string)
	var wpWg sync.WaitGroup
	scanTicker := time.NewTicker(time.Duration(p.scanInterval) * time.Second)
	defer scanTicker.Stop()
	ctx, cancel := context.WithCancel(context.Background())

PROCESSOR_LOOP:
	for {
		select {
		// An Aggregation worker pool closed due to inactivity
		case aggrID := <-workerClose:
			p.Log.Println("Deleting worker for " + aggrID)
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
				if keys, cursor, err = Redis.Scan(cursor, aggrKeyPrefix+"*", 50).Result(); err != nil {
					p.Log.Println(fmt.Errorf("Could not scan keys: %v", err))
					break
				}

				for _, ag := range keys {
					aggrID := strings.TrimPrefix(ag, aggrKeyPrefix)
					if _, ok := p.pools[aggrID]; !ok {

						aggr, err := GetAggregation(aggrID)
						if err != nil {
							p.Log.Printf("Could not get aggregation %s: %v", aggrID, err)
							continue
						}
						wp := NewWorkerPool(aggr)
						p.pools[aggrID] = &wp
						wpWg.Add(1)

						go func() {
							defer wpWg.Done()
							wp.Start(ctx, p.storageDir)
							// The processor only needs to be informed about non-forced close ( without context-cancel )
							if ctx.Err() == nil {
								workerClose <- wp.Aggr.ID
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
	return
}
