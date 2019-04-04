package httpbackend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/skroutz/downloader/job"
)

// DefaultClientTimeoutSec defines a default timeout in seconds for our http client
const DefaultClientTimeoutSec = 30

var (
	// Based on http.DefaultTransport
	//
	// See https://golang.org/pkg/net/http/#RoundTripper
	transport = &http.Transport{
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

	clientTimeout time.Duration
)

// Backend notifies about a job completion by executing an HTTP request.
type Backend struct {
	client  *http.Client
	reports chan job.Callback
}

// ID returns "http"
func (b *Backend) ID() string {
	return "http"
}

// Start starts the backend based on configuration provided by cfg.
func (b *Backend) Start(ctx context.Context, cfg map[string]interface{}) error {
	cfgTimeout, ok := cfg["timeout"]
	if !ok {
		clientTimeout = time.Duration(DefaultClientTimeoutSec) * time.Second
	} else {
		t, err := cfgTimeout.(json.Number).Int64()
		if err != nil {
			return err
		}
		clientTimeout = time.Duration(t) * time.Second
	}

	b.client = &http.Client{
		Transport: transport,
		Timeout:   clientTimeout, // Larger than Dial + TLS timeouts
	}

	b.reports = make(chan job.Callback)

	return nil
}

// Notify notifies a url about a job completion denoted by cbInfo.
func (b *Backend) Notify(url string, cbInfo job.Callback) error {
	payload, err := cbInfo.Bytes()
	if err != nil {
		cbInfo.Delivered = false
		cbInfo.DeliveryError = err.Error()
		return err
	}

	res, err := b.client.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil || res.StatusCode < 200 || res.StatusCode >= 300 {
		if err == nil {
			err = fmt.Errorf("Received Status: %s", res.Status)
		}
		cbInfo.Delivered = false
		cbInfo.DeliveryError = err.Error()
		return err
	}

	cbInfo.Delivered = true
	cbInfo.DeliveryError = ""
	b.reports <- cbInfo

	return nil
}

// DeliveryReports returns a channel of successfully emmited callbacks.
// Failures are returned directly by Notify() as errors.
func (b *Backend) DeliveryReports() <-chan job.Callback {
	return b.reports
}

// Stop shuts down the backend
func (b *Backend) Stop() error {
	close(b.reports)
	return nil
}
