//go:build go1.9
// +build go1.9

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/go-stack/stack"
	"github.com/skroutz/downloader/api"
	"github.com/skroutz/downloader/config"
	"github.com/skroutz/downloader/notifier"
	"github.com/skroutz/downloader/processor"
	"github.com/skroutz/downloader/processor/filestorage"
	"github.com/skroutz/downloader/storage"

	klog "github.com/go-kit/kit/log"
	"github.com/go-redis/redis"
	"github.com/urfave/cli"
)

var (
	sigCh   = make(chan os.Signal, 1)
	Version string
	cfg     config.Config
)

func main() {
	w := klog.NewSyncWriter(os.Stderr)
	logger := klog.NewLogfmtLogger(w)

	file := func() interface{} {
		return fmt.Sprintf("%s", stack.Caller(3))
	}

	lineno := func() interface{} {
		return fmt.Sprintf("%d", stack.Caller(3))
	}

	function := func() interface{} {
		return fmt.Sprintf("%n", stack.Caller(3))
	}

	logger = klog.With(logger, "ts", klog.DefaultTimestampUTC,
		"app", "downloader",
		"file", klog.Valuer(file),
		"lineno", klog.Valuer(lineno),
		"function", klog.Valuer(function))

	host, err := os.Hostname()
	if err != nil {
		logger.Log("event", "error", "msg", err)
	} else {
		logger = klog.With(logger, "host", host)
	}

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
				logger = klog.With(logger, "component", "api")

				signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

				useSentinel := !(cfg.Redis.MasterName == "" || len(cfg.Redis.Sentinel) == 0)

				var storage_o *storage.Storage
				if useSentinel {
					storage_o, err = storage.NewWithSentinel(cfg.Redis.Sentinel, cfg.Redis.MasterName, "api")
				} else {
					storage_o, err = storage.New(redisClient("api", cfg.Redis.Addr))
				}
				if err != nil {
					return err
				}
				api := api.New(storage_o, c.String("host"),
					c.Int("port"), cfg.API.HeartbeatPath, logger)

				go func() {
					logger.Log("action", "startup", "address", api.Server.Addr)
					err := api.Server.ListenAndServe()
					if err != nil && err != http.ErrServerClosed {
						logger.Log("level", "error", "msg", err)
						os.Exit(1)
					}
				}()

				if useSentinel {
					// Handle Sentinel failover
					failoverChan := make(chan bool)
					sentPubSub := storage_o.SentinelClient.PSubscribe("+switch-master")

					// Receive Sentinel +switch-master event and notify via failOverChan
					go storage.CheckSentinelFailover(sentPubSub, cfg.Redis.MasterName, failoverChan)

					for loop := true; loop; {
						select {
						case <-sigCh:
							// We need to exit
							loop = false
						case <-failoverChan:
							// We need to re-initiate a redis client
							logger.Log("SENTINEL", "+switch-master", "received")
							err := storage_o.ReEstablishRedisConnection(cfg.Redis.Sentinel, cfg.Redis.MasterName, "api")
							if err != nil {
								logger.Log("Could not re-establish redis connection after a Sentinel failover event: %s", err)
								loop = false
							}
						}
					}
				} else {
					<-sigCh
				}

				logger.Log("action", "shutdown", "action_phase", "start")
				err = api.Server.Shutdown(context.TODO())
				if err != nil {
					return err
				}
				logger.Log("action", "shutdown", "action_phase", "finish")
				return nil
			},
			Before: parseCliConfig,
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

				useSentinel := !(cfg.Redis.MasterName == "" || len(cfg.Redis.Sentinel) == 0)

				var storage_o *storage.Storage
				if useSentinel {
					storage_o, err = storage.NewWithSentinel(cfg.Redis.Sentinel, cfg.Redis.MasterName, "api")
				} else {
					storage_o, err = storage.New(redisClient("api", cfg.Redis.Addr))
				}
				if err != nil {
					return err
				}
				logger := log.New(os.Stderr, "[processor] ", log.Ldate|log.Ltime)

				var fstorage filestorage.FileStorage
				if cfg.Processor.StorageBackend == nil {
					fstorage = filestorage.NewFileSystem(cfg.Processor.StorageDir)
					logger.Println("WARNING: Using only storage_dir is deprecated. " +
						"Please declare a filestorage section.")
				} else {
					switch cfg.Processor.StorageBackend["type"] {
					case "s3":
						fstorage = filestorage.NewAWSS3(cfg.Processor.StorageBackend["region"],
							cfg.Processor.StorageBackend["bucket"])
					case "filesystem":
						fstorage = filestorage.NewFileSystem(cfg.Processor.StorageBackend["rootdir"])
					default:
						logger.Fatalf("Unknown filestorage type %s", cfg.Processor.StorageBackend["type"])
					}
				}
				processor, err := processor.New(storage_o, 3, cfg.Processor.StorageDir, logger, fstorage, cfg.Processor.DownloadURL)
				if err != nil {
					return err
				}
				processor.RequestHeaders = cfg.Processor.RequestHeaders

				if cfg.Processor.StatsInterval > 0 {
					processor.StatsIntvl = time.Duration(cfg.Processor.StatsInterval) * time.Millisecond
				}

				closeChan := make(chan struct{})
				go processor.Start(closeChan)

				if useSentinel {
					// Handle Sentinel failover
					failoverChan := make(chan bool)
					sentPubSub := storage_o.SentinelClient.PSubscribe("+switch-master")

					// Receive Sentinel +switch-master event and notify via failOverChan
					go storage.CheckSentinelFailover(sentPubSub, cfg.Redis.MasterName, failoverChan)

					for loop := true; loop; {
						select {
						case <-sigCh:
							// We need to exit
							loop = false
						case <-failoverChan:
							// We need to re-initiate a redis client
							logger.Println("SENTINEL", "+switch-master", "received")
							err := storage_o.ReEstablishRedisConnection(cfg.Redis.Sentinel, cfg.Redis.MasterName, "api")
							if err != nil {
								logger.Printf("Could not re-establish redis connection after a Sentinel failover event: %s\n", err)
								loop = false
							}
						}
					}
				} else {
					<-sigCh
				}

				processor.Log.Println("Shutting down...")
				closeChan <- struct{}{}
				processor.Log.Println("Waiting for worker pools to shut down...")
				<-closeChan
				processor.Log.Println("Bye!")
				return nil
			},
			Before: parseCliConfig,
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

				useSentinel := !(cfg.Redis.MasterName == "" || len(cfg.Redis.Sentinel) == 0)

				var storage_o *storage.Storage
				if useSentinel {
					storage_o, err = storage.NewWithSentinel(cfg.Redis.Sentinel, cfg.Redis.MasterName, "api")
				} else {
					storage_o, err = storage.New(redisClient("api", cfg.Redis.Addr))
				}
				if err != nil {
					return err
				}
				logger := log.New(os.Stderr, "[notifier] ", log.Ldate|log.Ltime)
				notifier, err := notifier.New(storage_o, cfg.Notifier.Concurrency, logger)
				if err != nil {
					logger.Fatal(err)
				}

				if cfg.Notifier.StatsInterval > 0 {
					notifier.StatsIntvl = time.Duration(cfg.Notifier.StatsInterval) * time.Millisecond
				}

				if cfg.Notifier.DeletionInterval > 0 {
					notifier.DeletionIntvl = time.Duration(cfg.Notifier.DeletionInterval) * time.Minute
				} else {
					logger.Fatalf("You need to provide a positive integer for the deletion_interval setting. " +
						"The setting is important and represents the amount of time in minutes, that a file " +
						"will be scheduled for deletion, after a job's callback has been delivered successfully.")
				}

				closeChan := make(chan struct{})
				go notifier.Start(closeChan, cfg.Backends)

				if useSentinel {
					// Handle Sentinel failover
					failoverChan := make(chan bool)
					sentPubSub := storage_o.SentinelClient.PSubscribe("+switch-master")

					// Receive Sentinel +switch-master event and notify via failOverChan
					go storage.CheckSentinelFailover(sentPubSub, cfg.Redis.MasterName, failoverChan)

					for loop := true; loop; {
						select {
						case <-sigCh:
							// We need to exit
							loop = false
						case <-failoverChan:
							// We need to re-initiate a redis client
							logger.Println("SENTINEL", "+switch-master", "received")
							err := storage_o.ReEstablishRedisConnection(cfg.Redis.Sentinel, cfg.Redis.MasterName, "api")
							if err != nil {
								logger.Printf("Could not re-establish redis connection after a Sentinel failover event: %s\n", err)
								loop = false
							}
						}
					}
				} else {
					<-sigCh
				}

				notifier.Log.Println("Shutting down...")
				closeChan <- struct{}{}
				notifier.Log.Println("Waiting for notifier to shut down...")
				<-closeChan
				notifier.Log.Println("Bye!")
				return nil
			},
			Before: parseCliConfig,
		},
		cli.Command{
			Name:  "version",
			Usage: "Show downloader version",
			Action: func(c *cli.Context) error {
				_, err := fmt.Fprintf(os.Stdout, "Downloader Version %s\n", Version)
				return err
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func parseCliConfig(ctx *cli.Context) error {
	var err error
	cfg, err = config.Parse(ctx.String("config"))
	return err
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
