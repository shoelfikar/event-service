// Package streamcache caches stream identity lookups
// (flussonic_stream_name → {id, tenant_id, name}) to remove the per-event DB
// query in the hot path and the N+1 lookup inside summary rebuilds. Streams
// change rarely, so a short TTL is safe.
package streamcache

import (
	"context"
	"sync"
	"time"

	"github.com/streamforge/event-service/internal/store"
)

type entry struct {
	ref       store.StreamRef
	found     bool
	expiresAt time.Time
}

type Cache struct {
	st  *store.Store
	ttl time.Duration

	mu sync.RWMutex
	m  map[string]entry
}

func New(st *store.Store, ttl time.Duration) *Cache {
	return &Cache{st: st, ttl: ttl, m: make(map[string]entry)}
}

// Resolve returns the stream identity for a Flussonic name, caching both hits
// and misses for the TTL window.
func (c *Cache) Resolve(ctx context.Context, name string) (store.StreamRef, bool, error) {
	c.mu.RLock()
	if e, ok := c.m[name]; ok && e.expiresAt.After(time.Now()) {
		c.mu.RUnlock()
		return e.ref, e.found, nil
	}
	c.mu.RUnlock()

	ref, found, err := c.st.ResolveStream(ctx, name)
	if err != nil {
		return store.StreamRef{}, false, err
	}
	c.mu.Lock()
	c.m[name] = entry{ref: ref, found: found, expiresAt: time.Now().Add(c.ttl)}
	c.mu.Unlock()
	return ref, found, nil
}

// Name is a redisstate.NameResolver: returns the human-readable stream name,
// or "" if unknown. Never blocks on errors.
func (c *Cache) Name(ctx context.Context, name string) string {
	ref, found, err := c.Resolve(ctx, name)
	if err != nil || !found {
		return ""
	}
	return ref.Name
}
