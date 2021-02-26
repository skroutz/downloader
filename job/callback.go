package job

import (
	"encoding/json"
)

// Callback holds info to be posted back to the provided callback destination.
type Callback struct {
	// Success refers to whether a job download was successful or not
	Success bool `json:"success"`

	// Error contains errors that occured during a job download
	Error string `json:"error"`

	// Extra are opaque/pass through data
	Extra string `json:"extra"`

	// ImageSize if supported
	ImageSize string `json:"image_size"`

	// ResourceURL is the url of the requested for download resource
	ResourceURL string `json:"resource_url"`

	// DownlaodURL the url where the downloaded resource resides
	DownloadURL string `json:"download_url"`

	// JobID is the unique id of a Job
	JobID string `json:"job_id"`

	// ResponseCode is the http response for the downloaded resource e.g 200, 404
	ResponseCode int `json:"response_code"`

	// Delivered signifies where the callback has been delivered or not
	Delivered bool `json:"delivered"`

	// DeliveryError contains the error occured while delivering a callback
	DeliveryError string `json:"delivery_error"`
}

// Bytes returns a byte slice for a callback info encoded as JSON
func (cb *Callback) Bytes() ([]byte, error) {
	return json.Marshal(cb)
}
