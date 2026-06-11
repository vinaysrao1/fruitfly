// Package cluster implements entity-affinity routing across fruitfly peers.
//
// Every peer runs the same binary with the same static peer list. Ownership
// of a routing key is decided by rendezvous (highest-random-weight) hashing:
// the owner is the peer with the highest hash(peer, key). Rendezvous hashing
// needs no ring state, no virtual nodes, and moves only ~1/N of keys when
// the peer list changes by one.
package cluster

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"net/http"
	"time"
)

const forwardTimeout = 5 * time.Second

// Router forwards events to the peer that owns their routing key.
type Router struct {
	self   string // this process's own entry in peers
	peers  []string
	client *http.Client
}

// NewRouter builds a router from the static peer list. self must be one of
// peers (it identifies this process); peers must contain at least one entry.
func NewRouter(self string, peers []string) (*Router, error) {
	if len(peers) == 0 {
		return nil, fmt.Errorf("cluster: peer list is empty")
	}
	found := false
	for _, p := range peers {
		if p == self {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("cluster: self %q not in peer list %v", self, peers)
	}
	return &Router{
		self:  self,
		peers: peers,
		client: &http.Client{
			Timeout: forwardTimeout,
		},
	}, nil
}

// Owner returns the peer owning key and whether that peer is this process.
func (r *Router) Owner(key string) (peer string, self bool) {
	keyHash := fnvSum(key)
	var best string
	var bestScore uint64
	for _, p := range r.peers {
		// Combine independent peer and key hashes through a strong 64-bit
		// finalizer. Hashing peer+key in one FNV stream is NOT independent
		// per peer (the shared key suffix preserves the prefix ordering),
		// which lets one peer win rendezvous for nearly every key.
		score := mix64(fnvSum(p) ^ keyHash)
		if best == "" || score > bestScore {
			best, bestScore = p, score
		}
	}
	return best, best == r.self
}

func fnvSum(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

// mix64 is the splitmix64 finalizer: full-avalanche mixing of a 64-bit word.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// Forward sends body to peer's ingest endpoint, marked as forwarded so the
// receiver processes it locally regardless of its own ring view (one-hop
// bound). batch selects the NDJSON batch endpoint.
func (r *Router) Forward(ctx context.Context, peer string, body []byte, batch bool) error {
	url := peer + "/events"
	if batch {
		url += "/batch"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Fruitfly-Forwarded", "1")

	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("peer %s returned %d", peer, resp.StatusCode)
	}
	return nil
}
