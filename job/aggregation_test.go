package job

import (
	"fmt"
	"testing"
)

func TestAggregationUnmarshal(t *testing.T) {
	tc := map[string]bool{
		// id
		`{"aggr_id":"foo", "aggr_limit":4, "url":"http://foobar.com","callback_url":"http://foo.bar","extra":"whatever"}`: false,
		`{"aggr_id":4, "aggr_limit":4, "url":"http://foobar.com","callback_url":"http://foo.bar","extra":"whatever"}`:     true,
		`{"aggr_id":"", "aggr_limit":4, "url":"http://foobar.com","callback_url":"http://foo.bar","extra":"whatever"}`:    true,

		// limit
		`{"aggr_id":"limitfoo", "aggr_limit":4, "url":"http://foobar.com","callback_url":"http://foo.bar","extra":"whatever"}`:   false,
		`{"aggr_id":"limitbar", "aggr_limit":-2, "url":"http://foobar.com","callback_url":"http://foo.bar","extra":"whatever"}`:  true,
		`{"aggr_id":"limitbaz", "aggr_limit":"4", "url":"http://foobar.com","callback_url":"http://foo.bar","extra":"whatever"}`: true,
		`{"aggr_id":"limitqux", "url":"http://foobar.com","callback_url":"http://foo.bar","extra":"whatever"}`:                   true,

		// proxy
		`{"aggr_id":"proxyfoo", "aggr_limit":4, "url":"http://foobar.com","callback_url":"http://foo.bar","extra":"whatever"}`:                                     false,
		`{"aggr_id":"proxybar", "aggr_limit":4, "aggr_proxy":"", "url":"http://foobar.com","callback_url":"http://foo.bar","extra":"whatever"}`:                    false,
		`{"aggr_id":"proxyqux", "aggr_limit":4, "aggr_proxy":"https://example.org", "url":"http://foobar.com","callback_url":"http://foo.bar","extra":"whatever"}`: false,
		`{"aggr_id":"proxybaz", "aggr_limit":4, "aggr_proxy":null, "url":"http://foobar.com","callback_url":"http://foo.bar","extra":"whatever"}`:                  true,
		`{"aggr_id":"proxyquux", "aggr_limit":4, "aggr_proxy":"example", "url":"http://foobar.com","callback_url":"http://foo.bar","extra":"whatever"}`:            true,
		`{"aggr_id":"proxycorge", "aggr_limit":4, "aggr_proxy":4, "url":"http://foobar.com","callback_url":"http://foo.bar","extra":"whatever"}`:                   true,
	}

	for data, expectErr := range tc {
		aggr := new(Aggregation)
		err := aggr.UnmarshalJSON([]byte(data))
		receivedErr := (err != nil)
		if receivedErr != expectErr {
			if err != nil {
				fmt.Println(err)
			}
			t.Errorf("Expected receivedErr to be %v for '%s'", expectErr, data)
		}
	}
}
