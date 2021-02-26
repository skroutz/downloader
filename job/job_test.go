package job

import (
	"fmt"
	"testing"
)

func TestUnmarshalJSON(t *testing.T) {
	tc := map[string]bool{
		``:              true,
		`{"foo"}`:       true,
		`{"foo":"bar"}`: true,

		// invalid url
		`{"aggr_id":"foo", "url":"foo","callback_url":"http://foo.bar","extra":"whatever"}`: true,
		`{"aggr_id":"foo", "url":"","callback_url":"http://foo.bar","extra":"whatever"}`:    true,

		// invalid cb url
		`{"aggr_id":"foo", "url":"http://foobar.com","callback_url":"fijfij","extra":"whatever"}`: true,

		// invalid aggr_id
		`{"aggr_id":true, "url":"http://foobar.com","callback_url":"http://foo.bar","extra":"whatever"}`: true,
		`{"aggr_id":"", "url":"http://foobar.com","callback_url":"http://foo.bar","extra":"whatever"}`:   true,

		`{"aggr_id":"foo","url":"http://foobar.com","callback_url":"http://foo.bar"}`:                     false,
		`{"aggr_id":"foo", "url":"http://foobar.com","callback_url":"http://foo.bar","extra":"whatever"}`: false,
		`{"aggr_id":"foo","url":"http://foobar.com","callback_url":"http://foo.bar","extra":""}`:          false,

		// timeout
		`{"aggr_id":"timeoutfoo", "download_timeout":12, "url":"http://foobar.com","callback_url":"http://foo.bar","extra":"whatever"}`:   false,
		`{"aggr_id":"timeoutfoo", "url":"http://foobar.com","callback_url":"http://foo.bar","extra":"whatever"}`:                          false,
		`{"aggr_id":"timeoutfoo", "download_timeout":null, "url":"http://foobar.com","callback_url":"http://foo.bar","extra":"whatever"}`: true,
		`{"aggr_id":"timeoutfoo", "download_timeout":0, "url":"http://foobar.com","callback_url":"http://foo.bar","extra":"whatever"}`:    true,
		`{"aggr_id":"timeoutfoo", "download_timeout":-2, "url":"http://foobar.com","callback_url":"http://foo.bar","extra":"whatever"}`:   true,
		`{"aggr_id":"timeoutfoo", "download_timeout":"4", "url":"http://foobar.com","callback_url":"http://foo.bar","extra":"whatever"}`:  true,

		// request headers
		`{"aggr_id":"requestheadersfoo", "url":"http://foobar.com", "callback_url":"http://foo.bar",
		"extra": "whatever", "request_headers": {"User-Agent":"Test-Agent"}}`: false,
		`{"aggr_id":"requestheadersfoo", "url":"http://foobar.com",
		"callback_url":"http://foo.bar", "extra": "whatever",
		"request_headers": {"Accept-Encoding":"gzip","User-Agent":"Test-Agent"}}`: false,
		`{"aggr_id":"requestheadersfoo", "user_agent":"Test-Agent",
		"url":"http://foobar.com", "callback_url":"http://foo.bar",
		"extra": "whatever", "request_headers": 1}`: true,
	}

	for data, expectErr := range tc {
		j := new(Job)
		err := j.UnmarshalJSON([]byte(data))
		receivedErr := (err != nil)
		if receivedErr != expectErr {
			if err != nil {
				fmt.Println(err)
			}
			t.Errorf("Expected receivedErr to be %v for '%s'", expectErr, data)
		}
	}
}

func TestJobToString(t *testing.T) {
	testJob := Job{}
	res := testJob.String()
	expected := "Job{ID:, Aggr:, URL:, ExtractImageSize:false, ImageSize: , "+
		"callback_url:, callback_type:, callback_dst:, Timeout:0, RequestHeaders:map[]}"

	if res != expected {
		t.Errorf("Expected '%s', got '%s'", expected, res)
	}
}
