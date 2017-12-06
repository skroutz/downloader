package diskcheck

import (
	"context"
	"sync"
	"syscall"
	"testing"
	"time"
)

func restoreStatfs() {
	statfs = syscall.Statfs
}

func fullStatfs(path string, buf *syscall.Statfs_t) (err error) {
	buf.Bsize = 4096
	buf.Blocks = 1000
	buf.Bfree = 0
	return
}

func emptyStatfs(path string, buf *syscall.Statfs_t) (err error) {
	buf.Bsize = 4096
	buf.Blocks = 1000
	buf.Bfree = 1000
	return
}

// We define a "flaky" disk as a disk that changes its disk capacity based on
// the previous disk state.
type flakyfs struct {
	last Health
}

// Statfs uses the "flaky" disk to mock a disk lifetime in regards to its
// fullness.
//
// It changes the "flaky" disk state on a cyclic manner in every call, from
// healthy to sick and the opposite.
func (f *flakyfs) Statfs(path string, buf *syscall.Statfs_t) (err error) {
	buf.Bsize = 4096
	buf.Blocks = 1000
	if f.last == Sick {
		buf.Bfree = 0
		f.last = Healthy
	} else {
		buf.Bfree = buf.Blocks
		f.last = Sick
	}
	return
}

func TestEmptyDisk(t *testing.T) {
	statfs = emptyStatfs
	defer restoreStatfs()

	c, err := New("/notexists", 90, 60, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("Error initializing disk checker: %q", err)
	}
	ctx, cancel := context.WithCancel(context.TODO())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Run(ctx)
	}()
	time.Sleep(20 * time.Millisecond)

	// Test that the disk remains healthy.
	select {
	case state := <-c.C():
		t.Fatalf("Received unexpected %q", state)
	default:
	}

	cancel()
	wg.Wait()
}

func TestFullDisk(t *testing.T) {
	statfs = fullStatfs
	defer restoreStatfs()

	c, err := New("/notexists", 90, 60, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("Error initializing disk checker: %q", err)
	}
	ctx, cancel := context.WithCancel(context.TODO())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Run(ctx)
	}()
	state := <-c.C()
	if state != Sick {
		t.Fatalf("Expected: %q but got: %q", Sick, state)
	}

	// Test that the disk state doesn't change while the disk remains full.
	time.Sleep(20 * time.Millisecond)
	select {
	case state = <-c.C():
		t.Fatalf("Received unexpected %q", state)
	default:
	}

	cancel()
	wg.Wait()
}

func TestFlakyDisk(t *testing.T) {
	var f flakyfs
	statfs = f.Statfs
	defer restoreStatfs()

	c, err := New("/notexists", 90, 60, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("Error initializing disk checker: %q", err)
	}
	ctx, cancel := context.WithCancel(context.TODO())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Run(ctx)
	}()

	state := <-c.C()
	if state != Sick {
		t.Fatalf("Expected: %q but got: %q", Sick, state)
	}
	state = <-c.C()
	if state != Healthy {
		t.Fatalf("Expected: %q but got: %q", Healthy, state)
	}

	cancel()
	wg.Wait()
}
