package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/urfave/cli"
)

var (
	sigCh = make(chan os.Signal, 1)
	cfg   Config
)

func main() {
	app := cli.NewApp()
	app.Name = "downloader"
	app.Usage = "RateLimited Async download API"
	app.HideVersion = true

	app.Commands = cli.Commands{
		cli.Command{
			Name: "api",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "host",
					Usage: "`HOST` to listen on",
					Value: "0.0.0.0",
				},
				cli.IntFlag{
					Name:  "port, p",
					Usage: "`PORT` to listen on",
					Value: 80,
				},
				cli.StringFlag{
					Name:  "config, c",
					Usage: "`FILE` to load config from",
					Value: "config.json",
				},
			},
			Action: func(c *cli.Context) error {
				s := newServer(c.String("host"), c.Int("port"))
				return s.ListenAndServe()
			},
			Before: BeforeCommand,
		},
	}

	log.Println(app.Run(os.Args))
}

// BeforeCommand extracts configuration from the provided config file and initializes redis
func BeforeCommand(c *cli.Context) error {
	f, err := os.Open(c.String("config"))
	if err != nil {
		return err
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	dec.UseNumber()
	err = dec.Decode(&cfg)
	if err != nil {
		return err
	}

	return InitStorage(cfg.Redis.Host, cfg.Redis.Port)
}
