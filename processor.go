package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var client *http.Client

const defaultScanInterval = 3

func init() {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{},
	}
	client = &http.Client{Transport: tr, Timeout: time.Duration(3) * time.Second}
}

// Processor is in charge of assigning worker pools to aggregations found in Redis and instrumenting them
type Processor struct {
	StorageDir   string
	scanInterval int
	pool         map[string]*WorkerPool
}

// WorkerPool manages download goroutines and is responsible of enforcing rate limit quotas on its
// aggregation
type WorkerPool struct {
	Aggr          Aggregation
	activeWorkers int32
	jobChan       chan Job
	shutdown      chan struct{}
}

// NewWorkerPool creates a WorkerPool for the given Aggregation
func NewWorkerPool(aggr Aggregation) WorkerPool {
	return WorkerPool{
		activeWorkers: 0,
		Aggr:          aggr,
		jobChan:       make(chan Job, 1),
	}
}

// IncreaseWorkers wraps atomic addition on ActiveWorkers counter
func (wp *WorkerPool) IncreaseWorkers() {
	atomic.AddInt32(&wp.activeWorkers, 1)
}

// DecreaseWorkers wraps atomic deduction on ActiveWorkers counter
func (wp *WorkerPool) DecreaseWorkers() {
	atomic.AddInt32(&wp.activeWorkers, -1)
}

// ActiveWorkers wraps the atomic load operation and return the currently active
// workers of the pool
func (wp *WorkerPool) ActiveWorkers() int {
	return int(atomic.LoadInt32(&wp.activeWorkers))
}

// Start encapsulates the main WorkerPool logic.
// All Goroutine spawning, Job popping from Redis and signal handling is performed here.
func (wp *WorkerPool) Start(ctx context.Context, savedir string) {
	log.Printf("[WorkerPool %s] Working", wp.Aggr.ID)
	var wg sync.WaitGroup

WORKERPOOL_LOOP:
	for {
		select {
		case <-ctx.Done():
			log.Printf("[WorkerPool %s] Received shutdown", wp.Aggr.ID)
			close(wp.jobChan)
			break WORKERPOOL_LOOP
		default:
			job, err := PopJob(jobKeyPrefix + wp.Aggr.ID)
			if err != nil {
				if err.Error() != "Queue is empty" {
					log.Println(err)
				}
				continue
			}
			// Set StateInProgress to keep track of which jobs are queued for download
			job.SetState(StateInProgress)
			wp.jobChan <- job

			if wp.ActiveWorkers() < wp.Aggr.Limit {
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer wp.DecreaseWorkers()
					wp.IncreaseWorkers()
					wp.work(ctx, savedir)
				}()
			}
		}
	}

	wg.Wait()
	log.Printf("[WorkerPool %s] Closing", wp.Aggr.ID)
}

// work processes Jobs, consuming from the WorkerPool's jobChan
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

// NewProcessor acts as a constructor for the Processor struct
func NewProcessor(scaninter int, storageDir string) Processor {
	return Processor{
		StorageDir:   storageDir,
		scanInterval: scaninter,
		pool:         make(map[string]*WorkerPool),
	}
}

// Start orchestrates the downloader.
// It scans redis for new aggregations and creates worker pools to serve Jobs that belong to them.
func (p *Processor) Start(closeCh chan struct{}) {
	log.Println("[Processor] Starting")
	workerClose := make(chan string)
	scanTicker := time.NewTicker(time.Duration(p.scanInterval) * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	//defer the cancel call to free context resources in all possible cases
	defer cancel()

PROCESSOR_LOOP:
	for {
		select {
		// An Aggregation worker pool closed
		case aggrID := <-workerClose:
			log.Println("[Processor] Deleting worker for " + aggrID)
			delete(p.pool, aggrID)

		// Close signal from upper layer
		case <-closeCh:
			cancel()
			scanTicker.Stop()
		case <-scanTicker.C:
			var cursor uint64
			for {
				var keys []string
				var err error
				if keys, cursor, err = Redis.Scan(cursor, aggrKeyPrefix+"*", 50).Result(); err != nil {
					log.Println(fmt.Errorf("[Processor]Could not scan keys: %v", err))
					break
				}

				for _, ag := range keys {
					aggrID := strings.TrimPrefix(ag, aggrKeyPrefix)
					if _, ok := p.pool[aggrID]; !ok {

						aggr, err := GetAggregation(aggrID)
						if err != nil {
							log.Printf("[Processor] Could not get aggregation %s : %v", aggrID, err)
							continue
						}
						wp := NewWorkerPool(aggr)
						p.pool[aggrID] = &wp

						go func() {
							wp.Start(ctx, p.StorageDir)
							workerClose <- wp.Aggr.ID
						}()
					}
				}

				if cursor == 0 {
					break
				}
			}

		default:
			if ctx.Err() != nil {
				// our context has been canceled
				if len(p.pool) == 0 {
					break PROCESSOR_LOOP
				}
				continue

			}
		}
	}
	log.Println("[Processor] Closing")
	closeCh <- struct{}{}
	return
}
