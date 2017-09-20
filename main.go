package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

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
		cli.Command{
			Name: "processor",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "config, c",
					Usage: "`FILE` to load config from",
					Value: "config.json",
				},
			},
			Action: func(c *cli.Context) error {
				signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
				processor := NewProcessor(cfg.Processor.StorageDir)
				closechan := make(chan struct{})
				go processor.Start(closechan)

				<-sigCh
				log.Println("[Main] Received Shutdown signal")
				closechan <- struct{}{}
				log.Println("[Main] Waiting for Processor...")
				<-closechan
				return nil
			},
			Before: BeforeCommand,
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
	log.Println("Bye!")
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
