package job

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"

	"github.com/skroutz/downloader/processor/mimetype"
)

// State represents the download & callback states.
// For valid values see constants below.
type State string

// The available states of a job's DownloadState/CallbackState.
const (
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
	CallbackType  string `json:"callback_type"`
	CallbackDst   string `json:"callback_dst"`
	// TODO: Remove CallbackURL, in favor of CallbackType and CallbackDst, after
	// all users of Downloader have upgraded their request scheme.
	CallbackURL string `json:"callback_url"`

	// Auxiliary ad-hoc information used for debugging.
	CallbackMeta string `json:"-"`

	// How many times the callback request was attempted
	CallbackCount int `json:"-"`

	// Arbitrary info provided by the user that are posted
	// back during the callback
	Extra string `json:"extra"`

	// Response code of the download request
	ResponseCode int `json:"response_code"`

	// Mime type pattern provided by the client
	MimeType string `json:"mime_type"`

	// Http client timeout for download in seconds
	DownloadTimeout int `json:"download_timeout"`

	// The User-Agent to set in download requests
	UserAgent string `json:"user_agent"`
}

// MarshalBinary is used by redis driver to marshall custom type State
func (s State) MarshalBinary() (data []byte, err error) {
	return []byte(string(s)), nil
}

// Path returns the relative job path
func (j *Job) Path() string {
	return path.Join(string(j.ID[0:3]), j.ID)
}

// UnmarshalJSON is used to populate a job from the values in
// the provided JSON message.
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
	if ok {
		_, err = url.ParseRequestURI(cbURL)
		if err != nil {
			return errors.New("Could not parse callback URL: " + err.Error())
		}
		j.CallbackURL = cbURL
	}

	// Check if callback_type and callback_dst are present
	// only if callback_url is empty
	if j.CallbackURL == "" {
		cbType, ok := tmp["callback_type"].(string)
		if !ok {
			return errors.New("callback_type must be a string")
		}

		cbDst, ok := tmp["callback_dst"].(string)
		if !ok {
			return errors.New("callback_dst must be a string")
		}

		if cbType == "" || cbDst == "" {
			return fmt.Errorf("You need to provide both callback_type (%#v) and callback_dst (%#v)", cbType, cbDst)
		}

		if strings.HasPrefix(cbDst, "http") {
			_, err = url.ParseRequestURI(cbDst)
			if err != nil {
				return errors.New("Could not parse URL: " + err.Error())
			}
		}

		j.CallbackType = cbType
		j.CallbackDst = cbDst
	}

	extra, ok := tmp["extra"].(string)
	if ok {
		j.Extra = extra
	}

	m, ok := tmp["mime_type"]
	if ok {
		// Since mime_type is optional, if it is not in the json doc
		// set it to the default ""
		if mime, ok := m.(string); ok {
			j.MimeType = mime
			err = mimetype.ValidateMimeTypePattern(mime)
		} else {
			err = errors.New("MimeType pattern must be a string")
		}
		if err != nil {
			return err
		}
	}

	var timeout int
	if timeoutS, ok := tmp["download_timeout"]; ok {
		timeoutf, ok := timeoutS.(float64) // The attribute is given, validate it.
		if !ok {
			return errors.New("Download timeout must be a number")
		}
		timeout = int(timeoutf)
		if timeout <= 0 {
			return errors.New("Download timeout must be greater than 0")
		}
	}
	j.DownloadTimeout = timeout

	var useragent string
	if useragentField, ok := tmp["user_agent"]; ok {
		useragent, ok = useragentField.(string)
		if !ok {
			return errors.New("UserAgent must be a string")
		}
	}
	j.UserAgent = useragent

	return nil
}

// CallbackInfo validates the state of a job and returns a callback info
// along with an error if appropriate. The expected argument downloadURL is
// the base path of a downloaded resource in the downloader.
func (j *Job) CallbackInfo(downloadURL url.URL) (Callback, error) {
	var dwURL string

	if j.DownloadState != StateSuccess && j.DownloadState != StateFailed {
		return Callback{}, fmt.Errorf("Invalid job download state: '%s'", j.DownloadState)
	}

	if j.DownloadState == StateSuccess {
		downloadURL.Path = path.Join(downloadURL.Path, j.Path())
		dwURL = downloadURL.String()
	}

	return Callback{
		Success:      j.DownloadState == StateSuccess,
		Error:        j.DownloadMeta,
		Extra:        j.Extra,
		ResourceURL:  j.URL,
		DownloadURL:  dwURL,
		JobID:        j.ID,
		ResponseCode: j.ResponseCode,
		Delivered:    true,
	}, nil
}

func (j Job) String() string {
	return fmt.Sprintf("Job{ID:%s, Aggr:%s, URL:%s, callback_url:%s, "+
		"callback_type:%s, callback_dst:%s, Timeout:%d, UserAgent:%s}",
		j.ID, j.AggrID, j.URL, j.CallbackURL, j.CallbackType, j.CallbackDst, j.DownloadTimeout, j.UserAgent)
}
