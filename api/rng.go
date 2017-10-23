package api

import (
	"encoding/base64"
	"math/rand"
	"sync"
)

// rng generates non-cryptophically-secure, random base64-encoded strings.
type rng struct {
	sync.Mutex
	src rand.Source

	gen *rand.Rand
	buf []byte
	enc *base64.Encoding
}

// newRNG returns an rng that generates n-random-byte strings, encoding based
// on enc.
func newRNG(n int, src rand.Source, enc *base64.Encoding) *rng {
	g := new(rng)
	g.buf = make([]byte, n, n)
	g.src = src
	g.gen = rand.New(g.src)
	g.enc = enc
	return g
}

// rand generates and returns a random string. It is safe for concurrent use by
// multiple goroutines.
func (r *rng) rand() string {
	defer r.Unlock()
	r.Lock()
	r.gen.Read(r.buf)
	return r.enc.EncodeToString(r.buf)
}
