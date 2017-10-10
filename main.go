package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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
	app.Usage = "Async rate-limited downloading service"
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
				signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
				l := log.New(os.Stderr, "[API] ", log.Ldate|log.Ltime)
				s := NewAPIServer(c.String("host"), c.Int("port"))
				go func() {
					l.Println(fmt.Sprintf("Listening on %s...", s.Addr))
					err := s.ListenAndServe()
					if err != nil && err != http.ErrServerClosed {
						l.Fatal(err)
					}
				}()

				<-sigCh
				l.Println("Shutting down gracefully...")
				err := s.Shutdown(context.TODO())
				if err != nil {
					return err
				}
				l.Println("Bye!")
				return nil
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
				l := log.New(os.Stderr, "[processor] ", log.Ldate|log.Ltime)
				processor := NewProcessor(3, cfg.Processor.StorageDir)
				closeChan := make(chan struct{})
				go processor.Start(closeChan)

				<-sigCh
				l.Println("Shutting down...")
				closeChan <- struct{}{}
				l.Println("Waiting for worker pools to finish...")
				<-closeChan
				l.Println("Bye!")
				return nil
			},
			Before: BeforeCommand,
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
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
