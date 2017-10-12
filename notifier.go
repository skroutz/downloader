package main

import (
	"crypto/tls"
	"log"
	"net/http"
	"sync"
	"time"
)

// Notifier is the the component responsible for consuming the result of jobs
// and notifying back the respective users by issuing HTTP requests to their callback URLs.
type Notifier struct {
	concurrency int
	client      *http.Client
	cbChan      chan Job
}

// NewNotifier takes the concurrency of the notifier as an argument
func NewNotifier(concurrency int) Notifier {
	return Notifier{
		concurrency: concurrency,
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{},
			},
			Timeout: time.Duration(3) * time.Second,
		},
		cbChan: make(chan Job),
	}
}

// Start starts the Notifier service.
// All callbacker instrumentation is performed here.
func (n *Notifier) Start(closeChan chan struct{}) {
	var wg sync.WaitGroup
	wg.Add(n.concurrency)
	for i := 0; i < n.concurrency; i++ {
		go func() {
			defer wg.Done()
			for job := range n.cbChan {
				job.PerformCallback(n.client)
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
			job, err := PopCallback()
			if err != nil {
				if err.Error() != "Queue is empty" {
					log.Println(err)
				}
				continue
			}
			n.cbChan <- job
		}
	}
}
