package stats

import (
	"context"
	"expvar"
	"log"
	"time"
)

type Stats struct {
	*expvar.Map
	interval   time.Duration
	reportfunc func(m *expvar.Map)
}

// Reporter encapsulates an expvar Map and acts as a metric reporting interface for each module
//var Reporter *Stats

// Run calls the report function of Stats using the specified interval/
// It shuts down when the provided context is cancelled
func (s *Stats) Run(ctx context.Context) {
	tick := time.NewTicker(s.interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Println("Stats Deamon Exiting")
			return
		case <-tick.C:
			s.reportfunc(s.Map)
		}
	}
}

// New initializes the Reporter and start Run
func New(id string, interval time.Duration, report func(*expvar.Map)) *Stats {
	return &Stats{expvar.NewMap(id), interval, report}
}
