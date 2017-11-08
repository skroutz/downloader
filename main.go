// +build go1.9

package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.skroutz.gr/skroutz/downloader/api"
	"golang.skroutz.gr/skroutz/downloader/notifier"
	"golang.skroutz.gr/skroutz/downloader/processor"
	"golang.skroutz.gr/skroutz/downloader/storage"

	"github.com/go-redis/redis"
	"github.com/urfave/cli"
)

var (
	sigCh  = make(chan os.Signal, 1)
	cfg    Config
	osArgs atomic.Value
)

func main() {
	app := cli.NewApp()
	app.Name = "downloader"
	app.Usage = "Async rate-limited downloading service"
	app.HideVersion = true

	app.Commands = cli.Commands{
		cli.Command{
			Name:  "api",
			Usage: "Start the API web server",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "host",
					Usage: "`HOST` to listen on",
					Value: "",
				},
				cli.IntFlag{
					Name:  "port, p",
					Usage: "`PORT` to listen on",
					Value: 8000,
				},
				cli.StringFlag{
					Name:  "config, c",
					Usage: "`FILE` to load config from",
					Value: "config.json",
				},
			},
			Action: func(c *cli.Context) error {
				signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

				storage, err := storage.New(redisClient("api", cfg.Redis.Addr))
				if err != nil {
					return err
				}
				logger := log.New(os.Stderr, "[api] ", log.Ldate|log.Ltime)
				api := api.New(storage, c.String("host"), c.Int("port"), cfg.API.HeartbeatPath, logger)

				go func() {
					logger.Println(fmt.Sprintf("Listening on %s...", api.Server.Addr))
					err := api.Server.ListenAndServe()
					if err != nil && err != http.ErrServerClosed {
						logger.Fatal(err)
					}
				}()

				<-sigCh
				logger.Println("Shutting down gracefully...")
				err = api.Server.Shutdown(context.TODO())
				if err != nil {
					return err
				}
				logger.Println("Bye!")
				return nil
			},
			Before: parseConfig,
		},
		cli.Command{
			Name:  "processor",
			Usage: "Start the job processor",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "config, c",
					Usage: "`FILE` to load config from",
					Value: "config.json",
				},
			},
			Action: func(c *cli.Context) error {
				signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

				client := &http.Client{
					Transport: &http.Transport{TLSClientConfig: &tls.Config{}},
					Timeout:   10 * time.Second}
				storage, err := storage.New(redisClient("processor", cfg.Redis.Addr))
				if err != nil {
					return err
				}
				logger := log.New(os.Stderr, "[processor] ", log.Ldate|log.Ltime)
				processor, err := processor.New(storage, 3, cfg.Processor.StorageDir, client, logger)
				if err != nil {
					return err
				}
				processor.UserAgent = cfg.Processor.UserAgent

				if cfg.Processor.StatsInterval > 0 {
					processor.StatsIntvl = time.Duration(cfg.Processor.StatsInterval) * time.Millisecond
				}

				closeChan := make(chan struct{})
				go processor.Start(closeChan)

				<-sigCh
				processor.Log.Println("Shutting down...")
				closeChan <- struct{}{}
				processor.Log.Println("Waiting for worker pools to shut down...")
				<-closeChan
				processor.Log.Println("Bye!")
				return nil
			},
			Before: parseConfig,
		},
		cli.Command{
			Name:  "notifier",
			Usage: "Start the notifier",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "config, c",
					Usage: "`FILE` to load config from",
					Value: "config.json",
				},
			},
			Action: func(c *cli.Context) error {
				signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

				storage, err := storage.New(redisClient("notifier", cfg.Redis.Addr))
				if err != nil {
					return err
				}
				logger := log.New(os.Stderr, "[notifier] ", log.Ldate|log.Ltime)
				notifier, err := notifier.New(storage, cfg.Notifier.Concurrency, logger, cfg.Notifier.DownloadURL)
				if err != nil {
					logger.Fatal(err)
				}

				if cfg.Notifier.StatsInterval > 0 {
					notifier.StatsIntvl = time.Duration(cfg.Notifier.StatsInterval) * time.Millisecond
				}

				closeChan := make(chan struct{})
				go notifier.Start(closeChan)

				<-sigCh
				notifier.Log.Println("Shutting down...")
				closeChan <- struct{}{}
				notifier.Log.Println("Waiting for notifier to shut down...")
				<-closeChan
				notifier.Log.Println("Bye!")
				return nil
			},
			Before: parseConfig,
		},
	}

	if err := app.Run(args()); err != nil {
		log.Fatal(err)
	}
}

// args is a wrapper around os.Args that allows us to override them
// for testing. Returns os.Args unless overriden.
func args() []string {
	override := osArgs.Load()
	if override != nil {
		return override.([]string)
	}

	return os.Args
}

// parseConfig extracts configuration from the provided config file and initializes redis
// TODO: make this an ordinary helper function so we can make it return
// a Storage and use it when we want to
func parseConfig(c *cli.Context) error {
	f, err := os.Open(c.String("config"))
	if err != nil {
		return err
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	dec.UseNumber()
	return dec.Decode(&cfg)
}

func redisClient(name, addr string) *redis.Client {
	setName := func(c *redis.Conn) error {
		ok, err := c.ClientSetName(name).Result()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("Error setting Redis client name to " + name)
		}
		return nil
	}
	return redis.NewClient(&redis.Options{
		Addr:       addr,
		OnConnect:  setName,
		MaxRetries: 5,
		PoolSize:   50 * runtime.NumCPU()})
}
