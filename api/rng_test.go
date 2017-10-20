package api

import (
	"encoding/base64"
	"math/rand"
	"testing"
	"time"
)

func TestRNG(t *testing.T) {
	numExecutions := 50
	numRandBytes := 10
	encoding := base64.RawURLEncoding
	expectedStrLen := encoding.EncodedLen(numRandBytes)

	generator := newRNG(numRandBytes,
		rand.NewSource(time.Now().UnixNano()),
		base64.RawURLEncoding)

	results := make(map[string]bool)
	ch := make(chan string)

	for i := 0; i < numExecutions; i++ {
		go func() {
			ch <- generator.rand()
		}()
	}

	for i := 0; i < numExecutions; i++ {
		s := <-ch
		if results[s] {
			t.Fatalf("Expected %s not to exist in %v", s, results)
		}
		if len(s) != expectedStrLen {
			t.Fatalf("Expected size to be %d, was %d", expectedStrLen, len(s))
		}
		results[s] = true
	}
}
