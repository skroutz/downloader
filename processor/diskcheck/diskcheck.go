// Package diskcheck provides a communication channel for checking the disk
// health.
package diskcheck

import (
	"context"
	"errors"
	"log"
	"syscall"
	"time"
)

const (
	// Healthy represents a disk usage below the given Threshold.
	Healthy Health = Health(true)

	// Sick represents a disk usage above the given Threshold.
	Sick = Health(false)
)

var statfs = syscall.Statfs

// Checker represents the downloader disk health component.
// The disk checker job is to notify its caller when the disk health state
// changes.
//
// The interface describes two main concepts:
// 	* Run: The component's main loop which loops between the health state
// 	functions. The running function implicates the current disk health.
//
// 	* C: The health state communication channel used by the processor.
//
// Useful implementation details:
// 	* waitForHealthy, waitForSick: health state monitoring functions that report
// 	  back to the channel when the state changes. They can be canceled with ctx.
//
// 	* diskUsage: the disk usage percentage.
//
// 	* Healthy/Sick: the disk health state.
//
// We need to stall the worker pools when the hd capacity drops below a
// threshold and restart them when we have again enough hd capacity.
// To achieve that, the processor reads from the communication channel the health
// state. Our approach implicitly considers that the disk checker will only
// write to the channel when there is a health state change.
//
// The health checking functions execution order determines the current health
// state, which enables us to keep the implementation free from saving states.
type Checker interface {
	Run(ctx context.Context)
	C() chan Health
}

// diskChecker represents a health state checker for the disk.
type diskChecker struct {

	// The check interval
	interval time.Duration

	// path is the location of the directory we will be checking its disk usage
	path string

	// disk usage thresholds (%)
	high, low diskUsage

	// The processor-diskChecker communication channel
	c chan Health
}

// diskUsage represents the disk usage percentage
// For example `diskUsage := 90` indicates that the disk usage is at 90%.
type diskUsage int

// Health represents the disk health state.
//
// We are using two constants to describe the disk health: Sick and Healthy.
type Health bool

func (h Health) String() string {
	if h == Healthy {
		return "healthy"
	}
	return "sick"
}

// New returns a new checker for the provided directory path and
// thresholds.
func New(path string, high int, low int, interval time.Duration) (Checker, error) {
	// Validate the input thresholds: 0 <= low < high <= 100
	if low >= high {
		return nil, errors.New("low threshold must be smaller than high")
	}
	if low < 0 || low > 100 {
		return nil, errors.New("low threshold must be between 0 and 100")
	}
	if high < 0 || high > 100 {
		return nil, errors.New("high threshold must be between 0 and 100")
	}
	// Validate that the checker can access the files directory statistics.
	// We build the validation around checkDiskUsage, which will return an
	// error if it can't get the system statistics.
	_, err := fetchDiskUsage(path)
	if err != nil {
		return nil, err
	}

	return &diskChecker{
		path:     path,
		high:     diskUsage(high),
		low:      diskUsage(low),
		interval: interval,
		c:        make(chan Health),
	}, nil
}

// C is the health state communication channel used by the processor.
//
// The processor reads from the communication channel the health state and may
// decide to act based on the health state.
//
// Example:
// The processor is in the "healthy" state and it receives from
// this channel the "sick" message. Therefore, it will stall the worker pools.
func (d *diskChecker) C() chan Health {
	return d.c
}

// Run informs its caller about the disk state.
//
// The caller can decide to act based on the returned disk health value.
// For example if the disk health is "sick" then the caller can decide to
// stall the workerPools.
//
// To decide on the disk health, it implicitly uses the current disk state.
// The health checking functions execution order determines the current health
// state.
//
// We authoritatively consider the disk healthy at start.
// The health checking functions are blocking the
// execution till the health state changes (e.g. the health goes from
// sick to healthy).
func (d *diskChecker) Run(ctx context.Context) {
	var err error
	for {
		// The error here represents that the "waitForSick" and the
		// "waitForHealthy" functions have received the canceled context.
		if err = d.waitForSick(ctx); err != nil {
			return
		}
		if err = d.waitForHealthy(ctx); err != nil {
			return
		}
	}
}

// waitForSick checks the disk health at regular intervals.
func (d *diskChecker) waitForSick(ctx context.Context) error {
	tick := time.NewTicker(d.interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			du, err := fetchDiskUsage(d.path)
			if err != nil {
				log.Printf("[diskcheck] Disk usage error in waitForSick: %v", err)
				continue
			}
			if du > d.high {
				d.c <- Sick
				return nil
			}
		}
	}
}

// waitForHealthy checks the disk health at regular intervals.
func (d *diskChecker) waitForHealthy(ctx context.Context) error {
	tick := time.NewTicker(d.interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			du, err := fetchDiskUsage(d.path)
			if err != nil {
				log.Printf("[diskcheck] Disk usage error in waitForHealthy: %v", err)
				continue
			}
			if du <= d.low {
				d.c <- Healthy
				return nil
			}
		}
	}
}

// fetchDiskUsage returns a new disk usage for the provided directory path.
func fetchDiskUsage(path string) (diskUsage, error) {
	fs := syscall.Statfs_t{}
	err := statfs(path, &fs)
	if err != nil {
		return 0, errors.New("Could not get file system statistics" + err.Error())
	}
	all := fs.Blocks * uint64(fs.Bsize)
	free := fs.Bfree * uint64(fs.Bsize)
	used := all - free
	usage := (float32(used) / float32(all)) * 100
	return diskUsage(usage), nil
}
