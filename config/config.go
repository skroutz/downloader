package config

import (
	"encoding/json"
	"os"
)

// Config holds the app's configuration
type Config struct {
	Redis struct {
		Addr string `json:"addr"`
	} `json:"redis"`

	API struct {
		HeartbeatPath string `json:"heartbeat_path"`
	} `json:"api"`

	Processor struct {
		StorageDir     string            `json:"storage_dir"`
		RequestHeaders map[string]string `json:"request_headers"`
		StatsInterval  int               `json:"stats_interval"`
	} `json:"processor"`

	Notifier struct {
		DownloadURL      string `json:"download_url"`
		Concurrency      int    `json:"concurrency"`
		StatsInterval    int    `json:"stats_interval"`
		DeletionInterval int    `json:"deletion_interval"`
	} `json:"notifier"`

	Backends map[string]map[string]interface{}
}

// Parse loads a given file name and creates a Configuration
func Parse(filename string) (Config, error) {
	cfg := Config{}
	f, err := os.Open(filename)
	if err != nil {
		return cfg, err
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	dec.UseNumber()
	return cfg, dec.Decode(&cfg)
}
