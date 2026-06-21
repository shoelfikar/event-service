package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// Many marks within the debounce window must collapse into a single rebuild.
func TestCoalescerCollapses(t *testing.T) {
	var calls int32
	c := newSummaryCoalescer(40*time.Millisecond, func(_ context.Context, _ string) {
		atomic.AddInt32(&calls, 1)
	})

	for i := 0; i < 50; i++ {
		c.mark("tenant-1")
		time.Sleep(time.Millisecond)
	}
	c.stop()

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected 1 coalesced rebuild, got %d", got)
	}
}

// Distinct tenants get independent rebuilds.
func TestCoalescerPerTenant(t *testing.T) {
	var calls int32
	c := newSummaryCoalescer(20*time.Millisecond, func(_ context.Context, _ string) {
		atomic.AddInt32(&calls, 1)
	})
	c.mark("a")
	c.mark("b")
	c.mark("c")
	c.stop()

	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("expected 3 rebuilds (one per tenant), got %d", got)
	}
}
