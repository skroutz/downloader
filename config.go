package main

import (
	"encoding/json"
	"os"
	"sync/atomic"

	"github.com/urfave/cli"
)

var cfg atomic.Value // *Config

// Config holds the app's configuration
type config struct {
	Redis struct {
		Addr string `json:"addr"`
	} `json:"redis"`

	API struct {
		HeartbeatPath string `json:"heartbeat_path"`
	} `json:"api"`

	Processor struct {
		StorageDir    string `json:"storage_dir"`
		UserAgent     string `json:"user_agent"`
		StatsInterval int    `json:"stats_interval"`
	} `json:"processor"`

	Notifier struct {
		DownloadURL   string `json:"download_url"`
		Concurrency   int    `json:"concurrency"`
		StatsInterval int    `json:"stats_interval"`
	} `json:"notifier"`
}

// Config returns the current configuration.
func Config() *config {
	c := cfg.Load()
	if c == nil {
		panic("config: No configuration found!")
	}

	return c.(*config)
}

func parseConfig(ctx *cli.Context) error {
	var c config

	f, err := os.Open(ctx.String("config"))
	if err != nil {
		return err
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	dec.UseNumber()
	dec.Decode(&c)

	cfg.Store(&c)
	return nil
}
