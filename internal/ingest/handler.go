// Package ingest exposes the HTTP endpoint Flussonic posts events to. It does
// the minimum synchronous work — verify signature, resolve tenant/stream,
// enqueue a job — then returns 200 immediately. Mirrors backendV2's
// WebhookController.handleEventSink + processEvent.
package ingest

import (
	"context"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"

	"github.com/streamforge/event-service/internal/model"
	"github.com/streamforge/event-service/internal/store"
)

const maxBodyBytes = 8 << 20 // 8 MiB

// StreamResolver resolves a Flussonic stream name to its identity.
// Satisfied by *streamcache.Cache.
type StreamResolver interface {
	Resolve(ctx context.Context, name string) (store.StreamRef, bool, error)
}

// Enqueuer submits an enriched event as a job. Satisfied by *worker.Enqueuer.
type Enqueuer interface {
	Enqueue(ctx context.Context, ev model.ProcessedEvent) error
}

type Handler struct {
	signKey  string
	cache    StreamResolver
	enqueuer Enqueuer
	log      *slog.Logger
}

func NewHandler(signKey string, cache StreamResolver, enqueuer Enqueuer, log *slog.Logger) *Handler {
	return &Handler{signKey: signKey, cache: cache, enqueuer: enqueuer, log: log}
}

// EventSink handles POST /webhooks/flussonic/event-sink.
func (h *Handler) EventSink(w http.ResponseWriter, r *http.Request) {
	rawBody, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		http.Error(w, "cannot read body", http.StatusBadRequest)
		return
	}

	if h.signKey != "" {
		sig := r.Header.Get("X-Signature")
		if sig == "" {
			http.Error(w, "Missing X-Signature header", http.StatusUnauthorized)
			return
		}
		expected := ComputeSignature(rawBody, h.signKey)
		if subtle.ConstantTimeCompare([]byte(expected), []byte(sig)) != 1 {
			h.log.Warn("invalid event_sink signature")
			http.Error(w, "Invalid signature", http.StatusUnauthorized)
			return
		}
	}

	// Flussonic sends an array of events; tolerate a single object too.
	var events []model.FlussonicEventPayload
	if len(rawBody) > 0 && rawBody[0] == '[' {
		if err := json.Unmarshal(rawBody, &events); err != nil {
			http.Error(w, "invalid JSON array", http.StatusBadRequest)
			return
		}
	} else {
		var one model.FlussonicEventPayload
		if err := json.Unmarshal(rawBody, &one); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		events = []model.FlussonicEventPayload{one}
	}

	processed := 0
	for i := range events {
		if h.handleOne(r.Context(), &events[i]) {
			processed++
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "processed": processed, "total": len(events),
	})
}

// handleOne resolves the stream and enqueues the enriched event. Returns false
// when the event is skipped (no stream name / unknown stream).
func (h *Handler) handleOne(ctx context.Context, p *model.FlussonicEventPayload) bool {
	streamName := p.Media
	if streamName == "" {
		return false
	}
	ref, found, err := h.cache.Resolve(ctx, streamName)
	if err != nil {
		h.log.Error("resolve stream failed", "stream", streamName, "err", err)
		return false
	}
	if !found {
		h.log.Warn("unknown stream", "stream", streamName)
		return false
	}

	eventName := p.Event
	if eventName == "" {
		eventName = "unknown"
	}
	sessionID := p.ID
	if sessionID == "" {
		sessionID = p.SessionID
	}

	h.log.Info("event received", "event", eventName, "ip", p.IP, "session", sessionID)

	ev := model.ProcessedEvent{
		Event:               eventName,
		Time:                p.Time,
		FlussonicStreamName: streamName,
		StreamID:            ref.ID,
		TenantID:            ref.TenantID,
		SessionID:           sessionID,
		IP:                  p.IP,
		Country:             p.Country,
		City:                p.City,
		Protocol:            p.Proto,
		UserAgent:           p.UserAgent,
		Bytes:               p.Bytes,
		Duration:            p.Duration,
		Bitrate:             p.Bitrate,
		UserID:              p.UserName,
		AdPath:              p.Path,
		AdPlacement:         p.Placement,
	}
	if err := h.enqueuer.Enqueue(ctx, ev); err != nil {
		h.log.Error("enqueue failed", "stream", streamName, "err", err)
		return false
	}
	return true
}

// ComputeSignature reproduces backendV2's event_sink signature:
// sha1_hex(rawBody || signKey). Exported for contract tests.
func ComputeSignature(rawBody []byte, signKey string) string {
	mac := sha1.Sum(append(append([]byte{}, rawBody...), []byte(signKey)...))
	return hex.EncodeToString(mac[:])
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
