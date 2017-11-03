package main

// Config holds the app's configuration
type Config struct {
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
