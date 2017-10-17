package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// Notifier is the the component responsible for consuming the result of jobs
// and notifying back the respective users by issuing HTTP requests to their
// provided callback URLs.
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
			job, err := PopCallback()
			if err != nil {
				if _, ok := err.(QueueEmptyError); ok {
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
func (n *Notifier) Notify(j *Job) {
	j.SetCallbackState(StateInProgress)
	cbInfo, err := j.callbackInfo()
	if err != nil {
		j.SetState(StateFailed, err.Error())
		return
	}

	cb, err := json.Marshal(cbInfo)
	if err != nil {
		j.SetState(StateFailed, err.Error())
		return
	}

	res, err := n.client.Post(j.CallbackURL, "application/json", bytes.NewBuffer(cb))
	if err != nil || res.StatusCode < 200 || res.StatusCode >= 300 {
		if err == nil {
			err = fmt.Errorf("Received Status: %s", res.Status)
		}
		j.SetCallbackState(StateFailed, err.Error())
		return
	}

	j.SetCallbackState(StateSuccess)
}
