package ingest_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/streamforge/event-service/internal/ingest"
	"github.com/streamforge/event-service/internal/model"
	"github.com/streamforge/event-service/internal/store"
)

// The stream all fixture events belong to.
const (
	fixtureMedia    = "3d7cd0db-3295-400a-957e-e5d57873172c"
	fixtureStreamID = "3d7cd0db-3295-400a-957e-e5d57873172c"
	fixtureTenantID = "227a090b-953c-4b86-8456-99859292e6c3"
)

// fakeResolver resolves only the known fixture stream; everything else misses.
type fakeResolver struct {
	known map[string]store.StreamRef
	calls int
}

func (f *fakeResolver) Resolve(_ context.Context, name string) (store.StreamRef, bool, error) {
	f.calls++
	ref, ok := f.known[name]
	return ref, ok, nil
}

// fakeEnqueuer records every enqueued event for assertions.
type fakeEnqueuer struct {
	mu     sync.Mutex
	events []model.ProcessedEvent
}

func (f *fakeEnqueuer) Enqueue(_ context.Context, ev model.ProcessedEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, ev)
	return nil
}

func newTestServer(t *testing.T, signKey string, resolver ingest.StreamResolver, enq ingest.Enqueuer) *httptest.Server {
	t.Helper()
	h := ingest.NewHandler(signKey, resolver, enq, slog.New(slog.NewTextHandler(io.Discard, nil)))
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/webhooks/flussonic/event-sink", h.EventSink)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func loadFixture(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "flussonic_events.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return raw
}

func knownStream() map[string]store.StreamRef {
	return map[string]store.StreamRef{
		fixtureMedia: {ID: fixtureStreamID, TenantID: fixtureTenantID, Name: "Live Channel 1"},
	}
}

func postEvents(t *testing.T, url string, body []byte, sig string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if sig != "" {
		req.Header.Set("X-Signature", sig)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// TestE2E_FlussonicEventSimulation replays a captured Flussonic event_sink burst
// against the ingest endpoint and asserts every event is parsed, mapped, and
// enqueued exactly once.
func TestE2E_FlussonicEventSimulation(t *testing.T) {
	body := loadFixture(t)

	var fixture []model.FlussonicEventPayload
	if err := json.Unmarshal(body, &fixture); err != nil {
		t.Fatalf("fixture is not valid JSON: %v", err)
	}
	wantTotal := len(fixture)

	resolver := &fakeResolver{known: knownStream()}
	enq := &fakeEnqueuer{}
	srv := newTestServer(t, "", resolver, enq)

	status, out := postEvents(t, srv.URL+"/api/v1/webhooks/flussonic/event-sink", body, "")

	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if int(out["total"].(float64)) != wantTotal {
		t.Errorf("total = %v, want %d", out["total"], wantTotal)
	}
	if int(out["processed"].(float64)) != wantTotal {
		t.Errorf("processed = %v, want %d (all events resolve to a known stream)", out["processed"], wantTotal)
	}
	if out["status"] != "ok" {
		t.Errorf("status field = %v, want ok", out["status"])
	}
	if len(enq.events) != wantTotal {
		t.Fatalf("enqueued %d events, want %d", len(enq.events), wantTotal)
	}

	// Verify field mapping fidelity on the first event.
	first := enq.events[0]
	src := fixture[0]
	if first.Event != src.Event {
		t.Errorf("event = %q, want %q", first.Event, src.Event)
	}
	if first.FlussonicStreamName != src.Media {
		t.Errorf("flussonic_stream_name = %q, want %q", first.FlussonicStreamName, src.Media)
	}
	if first.SessionID != src.ID {
		t.Errorf("session_id = %q, want %q (from `id`)", first.SessionID, src.ID)
	}
	if first.Protocol != src.Proto {
		t.Errorf("protocol = %q, want %q (from `proto`)", first.Protocol, src.Proto)
	}
	if first.StreamID != fixtureStreamID || first.TenantID != fixtureTenantID {
		t.Errorf("tenant/stream not injected from resolver: got stream=%q tenant=%q", first.StreamID, first.TenantID)
	}

	// Sanity: every enqueued event carries the resolved tenant/stream.
	for i, ev := range enq.events {
		if ev.TenantID != fixtureTenantID || ev.StreamID != fixtureStreamID {
			t.Fatalf("event %d missing resolved identity: %+v", i, ev)
		}
	}
}

// TestE2E_SignatureVerification covers the X-Signature contract with backendV2.
func TestE2E_SignatureVerification(t *testing.T) {
	const key = "test-sign-key"
	body := loadFixture(t)
	srv := newTestServer(t, key, &fakeResolver{known: knownStream()}, &fakeEnqueuer{})
	url := srv.URL + "/api/v1/webhooks/flussonic/event-sink"

	t.Run("valid signature accepted", func(t *testing.T) {
		sig := ingest.ComputeSignature(body, key)
		status, _ := postEvents(t, url, body, sig)
		if status != http.StatusOK {
			t.Fatalf("status = %d, want 200", status)
		}
	})

	t.Run("missing signature rejected", func(t *testing.T) {
		status, _ := postEvents(t, url, body, "")
		if status != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", status)
		}
	})

	t.Run("wrong signature rejected", func(t *testing.T) {
		status, _ := postEvents(t, url, body, "deadbeef")
		if status != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", status)
		}
	})
}

// TestE2E_UnknownStreamSkipped ensures events for unknown streams are skipped
// (not enqueued) while known ones still process.
func TestE2E_UnknownStreamSkipped(t *testing.T) {
	body := []byte(`[
		{"event":"play_opened","media":"` + fixtureMedia + `","id":"s1","ip":"1.1.1.1","proto":"hls"},
		{"event":"play_opened","media":"unknown-stream","id":"s2","ip":"2.2.2.2","proto":"hls"},
		{"event":"play_opened","media":"","id":"s3","ip":"3.3.3.3","proto":"hls"}
	]`)

	enq := &fakeEnqueuer{}
	srv := newTestServer(t, "", &fakeResolver{known: knownStream()}, enq)

	status, out := postEvents(t, srv.URL+"/api/v1/webhooks/flussonic/event-sink", body, "")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if int(out["total"].(float64)) != 3 {
		t.Errorf("total = %v, want 3", out["total"])
	}
	if int(out["processed"].(float64)) != 1 {
		t.Errorf("processed = %v, want 1 (only the known stream)", out["processed"])
	}
	if len(enq.events) != 1 {
		t.Fatalf("enqueued %d, want 1", len(enq.events))
	}
}

// TestE2E_SingleObjectPayload verifies a non-array body is accepted too.
func TestE2E_SingleObjectPayload(t *testing.T) {
	body := []byte(`{"event":"play_started","media":"` + fixtureMedia + `","id":"s9","ip":"9.9.9.9","proto":"hls"}`)
	enq := &fakeEnqueuer{}
	srv := newTestServer(t, "", &fakeResolver{known: knownStream()}, enq)

	status, out := postEvents(t, srv.URL+"/api/v1/webhooks/flussonic/event-sink", body, "")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if int(out["total"].(float64)) != 1 || int(out["processed"].(float64)) != 1 {
		t.Errorf("total/processed = %v/%v, want 1/1", out["total"], out["processed"])
	}
	if len(enq.events) != 1 || enq.events[0].SessionID != "s9" {
		t.Fatalf("unexpected enqueued events: %+v", enq.events)
	}
}
