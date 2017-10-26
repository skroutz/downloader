package job

import (
	"encoding/json"
	"errors"
	"net/url"
)

const (
	// The available states of a job's DownloadState/CallbackState.
	StatePending    = "Pending"
	StateFailed     = "Failed"
	StateSuccess    = "Success"
	StateInProgress = "InProgress"
)

// Job represents a user request for downloading a resource.
//
// It is the core entity of the downloader and holds all info and state of
// the download.
//
// TODO: should this be valid with an empty aggregation id?
type Job struct {
	// Auto-generated
	ID string `json:"-"`

	// The URL pointing to the resource to be downloaded
	URL string `json:"url"`

	// AggrID is the ID of the aggregation the job belongs to.
	//
	// TODO: should this be a pointer to an Aggregation? If so, then
	// NewJob should be a function on Aggregation
	AggrID string `json:"aggr_id"`

	DownloadState State `json:"-"`

	// How many times the download request was attempted
	DownloadCount int `json:"-"`

	// Auxiliary ad-hoc information. Typically used for communicating
	// download errors back to the user.
	DownloadMeta string `json:"-"`

	CallbackState State  `json:"-"`
	CallbackURL   string `json:"callback_url"`

	// Auxiliary ad-hoc information used for debugging.
	CallbackMeta string `json:"-"`

	// How many times the callback request was attempted
	CallbackCount int `json:"-"`

	// Arbitrary info provided by the user that are posted
	// back during the callback
	Extra string `json:"extra"`
}

// State represents the download & callback states.
// For valid values see constants below.
type State string

// MarshalBinary is used by redis driver to marshall custom type State
func (s State) MarshalBinary() (data []byte, err error) {
	return []byte(string(s)), nil
}

func (j *Job) UnmarshalJSON(b []byte) error {
	var tmp map[string]interface{}

	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}

	dlURL, ok := tmp["url"].(string)
	if !ok {
		return errors.New("URL must be a string")
	}
	_, err = url.ParseRequestURI(dlURL)
	if err != nil {
		return errors.New("Could not parse URL: " + err.Error())
	}
	j.URL = dlURL

	aggrID, ok := tmp["aggr_id"].(string)
	if !ok {
		return errors.New("aggr_id must be a string")
	}
	if aggrID == "" {
		return errors.New("aggr_id cannot be empty")
	}
	j.AggrID = aggrID

	cbURL, ok := tmp["callback_url"].(string)
	if !ok {
		return errors.New("callback_url must be a string")
	}
	_, err = url.ParseRequestURI(cbURL)
	if err != nil {
		return errors.New("Could not parse callback URL: " + err.Error())
	}
	j.CallbackURL = cbURL

	extra, ok := tmp["extra"].(string)
	if ok {
		j.Extra = extra
	}

	return nil
}
