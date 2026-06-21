package worker

import (
	"context"
	"sync"
	"time"
)

// summaryCoalescer collapses the per-event "rebuild tenant summary + publish
// SSE" work into at most one rebuild per tenant per debounce window. This is
// the key optimization: backendV2 rebuilt on every single play event, which is
// the dominant cost. Marking a tenant dirty (re)arms a timer; when it fires the
// rebuild+publish runs once for whatever accumulated.
type summaryCoalescer struct {
	debounce time.Duration
	fn       func(ctx context.Context, tenantID string)

	mu     sync.Mutex
	timers map[string]*time.Timer
	closed bool
	wg     sync.WaitGroup
}

func newSummaryCoalescer(debounce time.Duration, fn func(ctx context.Context, tenantID string)) *summaryCoalescer {
	return &summaryCoalescer{
		debounce: debounce,
		fn:       fn,
		timers:   make(map[string]*time.Timer),
	}
}

// mark schedules a rebuild for tenantID after the debounce window, coalescing
// repeated marks within that window into one run.
func (c *summaryCoalescer) mark(tenantID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	if t, ok := c.timers[tenantID]; ok {
		t.Reset(c.debounce)
		return
	}
	c.wg.Add(1)
	c.timers[tenantID] = time.AfterFunc(c.debounce, func() {
		defer c.wg.Done()
		c.mu.Lock()
		delete(c.timers, tenantID)
		c.mu.Unlock()
		c.fn(context.Background(), tenantID)
	})
}

// stop prevents new marks and waits for any in-flight rebuilds to finish.
func (c *summaryCoalescer) stop() {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.wg.Wait()
}
