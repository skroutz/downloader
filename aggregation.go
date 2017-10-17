package main

// Aggregation is the concept through which the rate limit rules are defined
// and enforced.
type Aggregation struct {
	ID string

	// Maximum numbers of concurrent download requests
	Limit int
}
