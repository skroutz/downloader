package job

import (
	"encoding/json"
	"errors"
	"net/url"
)

// Idle time after which an aggregation will become available for another processor
const AggregationTimeout = 300 // 5m in seconds

// Aggregation is the concept through which the rate limit rules are defined
// and enforced.
type Aggregation struct {
	ID string `json:"aggr_id"`

	// Maximum numbers of concurrent download requests
	Limit int `json:"aggr_limit"`

	// Proxy url for the client to use, optional
	Proxy string `json:"aggr_proxy"`

	// Unix Timestamp. Indicates if an aggregation is currently being processed.
	// If this is set, a new processor will not pick up the aggregation.
	ExpiresAt string `json:"-"`
}

// NewAggregation creates an aggregation with the provided ID and limit.
// If any of the prerequisites fail, an error is returned.
func NewAggregation(id string, limit int, proxy string, expiresAt string) (*Aggregation, error) {
	if id == "" {
		return nil, errors.New("Aggregation ID cannot be empty")
	}
	if limit <= 0 {
		return nil, errors.New("Aggregation limit must be greater than 0")
	}

	if proxy != "" {
		if _, err := url.ParseRequestURI(proxy); err != nil {
			return nil, errors.New("Aggregation proxy must be a valid URI: " + err.Error())
		}
	}

	return &Aggregation{ID: id, Limit: limit, Proxy: proxy, ExpiresAt: expiresAt}, nil
}

// UnmarshalJSON populates the aggregation with the values in the provided JSON.
func (a *Aggregation) UnmarshalJSON(b []byte) error {
	var tmp map[string]interface{}

	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}

	id, ok := tmp["aggr_id"].(string)
	if !ok {
		return errors.New("Aggregation ID must be a string")
	}
	if id == "" {
		return errors.New("Aggregation ID cannot be empty")
	}

	limitf, ok := tmp["aggr_limit"].(float64)
	if !ok {
		return errors.New("Aggregation limit must be a number")
	}

	limit := int(limitf)
	if limit <= 0 {
		return errors.New("Aggregation limit must be greater than 0")
	}

	var proxy string
	if proxyField, ok := tmp["aggr_proxy"]; ok {
		proxy, ok = proxyField.(string)
		if !ok {
			return errors.New("Aggregation proxy must be a string")
		}
		if proxy != "" {
			if _, err := url.ParseRequestURI(proxy); err != nil {
				return errors.New("Aggregation proxy must be a valid URI: " + err.Error())
			}
		}
	}

	var expiresAt string
	if expiresAtField, ok := tmp["expiresAt"]; ok {
		expiresAt, ok = expiresAtField.(string)
		if !ok {
			return errors.New("Aggregation expiresAt must be a string")
		}
	}

	a.ID = id
	a.Limit = limit
	a.Proxy = proxy
	a.ExpiresAt = expiresAt

	return nil
}
