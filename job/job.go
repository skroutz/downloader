package job

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strconv"
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

	// MaxRetries specifies the maximum number for retry attempts. It overrides
	// the default specified in processor.MaxDownloadRetries
	MaxRetries NullableInt `json:"-"`

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

	// Compute image size on supported mime-types
	ExtractImageSize bool `json:"extract_image_size"`

	// ImageSize of the downloaded image
	ImageSize string `json:"image_size"`

	// Http client timeout for download in seconds
	DownloadTimeout int `json:"download_timeout"`

	// The HTTP request headers provided by the user will be used for
	// downloading files.
	// This attribute is optional.
	RequestHeaders map[string]string `json:"request_headers,omitempty"`

	S3Bucket string `json:"s3_bucket"`
	S3Region string `json:"s3_region"`
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

	s3Region, ok := tmp["s3_region"].(string)
	if ok {
		j.S3Region = s3Region
	}

	s3Bucket, ok := tmp["s3_bucket"].(string)
	if ok {
		j.S3Bucket = s3Bucket
	}

	if s3Bucket == "" && s3Region != "" {
		return errors.New("s3_region provided without an s3_bucket")
	} else if s3Region == "" && s3Bucket != "" {
		return errors.New("s3_bucket provided without an s3_region")
	}

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
		if !ok && s3Bucket == "" {
			return errors.New("callback_type must be a string")
		}

		cbDst, ok := tmp["callback_dst"].(string)
		if !ok && s3Bucket == "" {
			return errors.New("callback_dst must be a string")
		}

		// Make callbacks optional if a client has provided their own S3 bucket
		if s3Bucket == "" && (cbType == "" || cbDst == "") {
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

	retriesJ, ok := tmp["max_retries"]
	if ok {
		retriesf, ok := retriesJ.(float64)
		if !ok {
			return fmt.Errorf("Max retries must be a number, was: %T", retriesJ)
		}
		retries := int(retriesf)
		if retries < 0 {
			return errors.New("Max retries must not be negative")
		}
		j.MaxRetries.Set(retries)
	}

	is, ok := tmp["extract_image_size"]
	// Default extract_image_size is false
	if ok {
		// Since image_size is optional, if it is not in the json doc
		// set it to the default ""
		if extractImageSize, ok := is.(bool); ok {
			j.ExtractImageSize = extractImageSize
		} else {
			err = errors.New("ExtractImageSize must be a boolean")
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

	rh, ok := tmp["request_headers"]
	if ok {
		if requestHeaders, ok := rh.(map[string]interface{}); ok {
			headers := make(map[string]string)
			for k, v := range requestHeaders {
				switch v.(type) {
				case string:
					headers[k] = v.(string)
				case int:
					headers[k] = strconv.Itoa(v.(int))
				default:
					// According to https://tools.ietf.org/html/rfc2616#section-5.3
					// all request headers can be represented as either integers
					// or strings. For that reason, we handle these. But in case,
					// a value is not of one of these types we raise an error.
					return fmt.Errorf(
						"Values of request headers keys must be either strings or ints."+
							"Given value is %s", v)
				}
			}
			j.RequestHeaders = headers
		} else {
			return errors.New("request_headers must be a dictionary")
		}
	}

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
		ImageSize:    j.ImageSize,
		Delivered:    true,
	}, nil
}

// HasCallback returns true if the job requires a callback.
// A valid case of a job not having a callback is if it has provided
// its own AWS S3 bucket for storage and hasn't specified a callback_type
// and a callback_dst or a callback_url
func (j *Job) HasCallback() bool {
	return j.CallbackURL != "" || j.CallbackDst != ""
}

func (j Job) String() string {
	return fmt.Sprintf("Job{ID:%s, Aggr:%s, URL:%s, "+
		"ExtractImageSize:%t, ImageSize: %s, "+
		"callback_url:%s, callback_type:%s, callback_dst:%s, Timeout:%d, RequestHeaders:%v}",
		j.ID, j.AggrID, j.URL,
		j.ExtractImageSize, j.ImageSize,
		j.CallbackURL, j.CallbackType, j.CallbackDst, j.DownloadTimeout, j.RequestHeaders)
}
