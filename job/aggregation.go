package job

import (
	"encoding/json"
	"errors"
)

// Aggregation is the concept through which the rate limit rules are defined
// and enforced.
type Aggregation struct {
	ID string `json:"aggr_id"`

	// Maximum numbers of concurrent download requests
	Limit int `json:"aggr_limit"`
}

// NewAggregation creates an aggregation with the provided ID and limit.
// If any of the prerequisites fail, an error is returned.
func NewAggregation(id string, limit int) (*Aggregation, error) {
	if id == "" {
		return nil, errors.New("Aggregation ID cannot be empty")
	}
	if limit <= 0 {
		return nil, errors.New("Aggregation limit must be greater than 0")
	}
	return &Aggregation{ID: id, Limit: limit}, nil
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

	a.ID = id
	a.Limit = limit

	return nil
}
