// Package stats provides stats reporting and storing abstractions.
package stats

import (
	"context"
	"expvar"
	"time"
)

// Stats is the statistics reporting entity of the downloader.
// It wraps an expvar.Map and acts as a deamon that reports stats using
// the provided reportfunc on the specified interval.
type Stats struct {
	*expvar.Map
	interval   time.Duration
	reportfunc func(m *expvar.Map)
}

// Run calls the report function of Stats using the specified interval/
// It shuts down when the provided context is cancelled
func (s *Stats) Run(ctx context.Context) {
	tick := time.NewTicker(s.interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			s.reportfunc(s.Map)
		}
	}
}

// New accepts an custom identifier for the thing being monitored, an interval
// on which to report the metrics and the report function.
func New(id string, interval time.Duration, report func(*expvar.Map)) *Stats {
	var statsMap *expvar.Map
	if val := expvar.Get(id); val != nil {
		if newmap, ok := val.(*expvar.Map); ok {
			statsMap = newmap
			statsMap.Init()
		}
	}
	if statsMap == nil {
		statsMap = expvar.NewMap(id)
	}

	// serve as liveness indicator for modules registering metrics
	statsMap.Add("alive", 1)

	return &Stats{statsMap, interval, report}
}
