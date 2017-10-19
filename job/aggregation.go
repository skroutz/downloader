package job

import (
	"errors"
)

// Aggregation is the concept through which the rate limit rules are defined
// and enforced.
type Aggregation struct {
	ID string

	// Maximum numbers of concurrent download requests
	Limit int
}

func NewAggregation(id string, limit int) (*Aggregation, error) {
	if id == "" {
		return nil, errors.New("Aggregation ID cannot be empty")
	}
	if limit <= 0 {
		return nil, errors.New("Aggregation limit must be greater than 0")
	}
	return &Aggregation{ID: id, Limit: limit}, nil
}
