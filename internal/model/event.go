// Package model defines the Flussonic event_sink payload types and the
// normalized ProcessedEvent that flows through the queue. These mirror
// backendV2's src/analytics/dto/flussonic-event.dto.ts exactly.
package model

import "regexp"

// FlussonicEventPayload is the raw payload Flussonic POSTs to the event_sink.
// Unknown fields are tolerated (Flussonic adds fields freely).
type FlussonicEventPayload struct {
	Event     string  `json:"event"`
	Media     string  `json:"media"` // flussonic stream name
	ID        string  `json:"id"`    // session id
	SessionID string  `json:"session_id"`
	Time      string  `json:"time"`
	IP        string  `json:"ip"`
	Country   string  `json:"country"`
	City      string  `json:"city"`
	Proto     string  `json:"proto"`
	Bytes     int64   `json:"bytes"`
	Duration  float64 `json:"duration"`
	UserAgent string  `json:"user_agent"`
	UserName  string  `json:"user_name"`
	Bitrate   float64 `json:"bitrate"`
	Path      string  `json:"path"`
	Placement string  `json:"placement"`
}

// ProcessedEvent is the enriched, tenant-resolved event enqueued as a job and
// consumed by the worker. Mirrors backendV2's ProcessedEvent interface.
type ProcessedEvent struct {
	Event               string  `json:"event"`
	Time                string  `json:"time,omitempty"`
	FlussonicStreamName string  `json:"flussonic_stream_name"`
	StreamID            string  `json:"stream_id"`
	TenantID            string  `json:"tenant_id"`
	SessionID           string  `json:"session_id,omitempty"`
	IP                  string  `json:"ip,omitempty"`
	Country             string  `json:"country,omitempty"`
	City                string  `json:"city,omitempty"`
	Protocol            string  `json:"protocol,omitempty"`
	UserAgent           string  `json:"user_agent,omitempty"`
	Bytes               int64   `json:"bytes,omitempty"`
	Duration            float64 `json:"duration,omitempty"`
	Bitrate             float64 `json:"bitrate,omitempty"`
	UserID              string  `json:"user_id,omitempty"`
	AdPath              string  `json:"ad_path,omitempty"`
	AdPlacement         string  `json:"ad_placement,omitempty"`
}

type EventType struct {
	Category string // play | source | stream | push | ingest | ad
	Action   string // opened | started | closed | updated | injected | connected
}

var eventRe = regexp.MustCompile(`^(play|source|stream|push|ingest|ad)_(opened|started|updated|closed|injected|connected)$`)

// ParseEventType splits a Flussonic event name like "play_started" into its
// category/action. Returns nil for unrecognized event names.
func ParseEventType(event string) *EventType {
	if event == "" {
		return nil
	}
	m := eventRe.FindStringSubmatch(event)
	if m == nil {
		return nil
	}
	return &EventType{Category: m[1], Action: m[2]}
}
