// Package worker consumes event jobs and performs all writes: session_events,
// stream_events, viewer_samples, ad_impressions, the per-stream Redis analytics
// hash, and (coalesced) the tenant summary + SSE publish. The per-event logic
// mirrors backendV2's AnalyticsEventProcessorService + BandwidthFetcherService.
package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/streamforge/event-service/internal/flussonic"
	"github.com/streamforge/event-service/internal/geoip"
	"github.com/streamforge/event-service/internal/model"
	"github.com/streamforge/event-service/internal/redisstate"
	"github.com/streamforge/event-service/internal/store"
	"github.com/streamforge/event-service/internal/streamcache"
	"github.com/streamforge/event-service/internal/ua"
)

const (
	statusLive    = "live"
	statusOffline = "offline"
)

type Processor struct {
	st        *store.Store
	state     *redisstate.State
	flu       *flussonic.Client
	geo       *geoip.Reader
	cache     *streamcache.Cache
	coalescer *summaryCoalescer
	log       *slog.Logger
}

func NewProcessor(
	st *store.Store,
	state *redisstate.State,
	flu *flussonic.Client,
	geo *geoip.Reader,
	cache *streamcache.Cache,
	debounce time.Duration,
	log *slog.Logger,
) *Processor {
	p := &Processor{st: st, state: state, flu: flu, geo: geo, cache: cache, log: log}
	p.coalescer = newSummaryCoalescer(debounce, p.rebuildAndPublish)
	return p
}

// Stop drains any pending coalesced rebuilds.
func (p *Processor) Stop() { p.coalescer.stop() }

// rebuildAndPublish is invoked by the coalescer (debounced per tenant).
func (p *Processor) rebuildAndPublish(ctx context.Context, tenantID string) {
	summary, err := p.state.RebuildTenantSummary(ctx, tenantID)
	if err != nil {
		p.log.Warn("rebuild summary failed", "tenant", tenantID, "err", err)
		return
	}
	if err := p.state.Publish(ctx, "sf:realtime-summary:"+tenantID, summary); err != nil {
		p.log.Warn("publish summary failed", "tenant", tenantID, "err", err)
	}
}

// Handle processes one enriched event. Mirrors handleEvent: log to
// stream_events, dispatch the category/action handler, then refresh bandwidth.
func (p *Processor) Handle(ctx context.Context, ev model.ProcessedEvent) error {
	et := model.ParseEventType(ev.Event)
	if et == nil {
		return nil // unrecognized — ack and drop, like backendV2
	}

	// 1. Activity log + realtime debug channel (StreamEventsService.record)
	details := eventDetails(ev)
	var createdAt *time.Time
	if ev.Time != "" {
		if t, err := time.Parse(time.RFC3339, ev.Time); err == nil {
			createdAt = &t
		}
	}
	if err := p.st.SaveStreamEvent(ctx, ev.StreamID, ev.TenantID, ev.Event, details, createdAt); err != nil {
		return err
	}
	_ = p.state.Publish(ctx, "sf:stream-events:"+ev.TenantID, map[string]any{
		"stream_id": ev.StreamID, "tenant_id": ev.TenantID, "event_type": ev.Event,
		"details": details, "created_at": time.Now().UTC().Format(time.RFC3339Nano),
	})

	// 2. Dispatch
	if err := p.dispatch(ctx, et, ev); err != nil {
		return err
	}

	// 3. Bandwidth refresh (BandwidthFetcherService.handleTrigger inline)
	p.fetchBandwidth(ctx, ev)
	return nil
}

func (p *Processor) dispatch(ctx context.Context, et *model.EventType, ev model.ProcessedEvent) error {
	switch et.Category {
	case "play":
		switch et.Action {
		case "opened":
			return p.st.SaveSessionOpen(ctx, sessionData(ev))
		case "started":
			return p.playStarted(ctx, ev)
		case "closed":
			return p.playClosed(ctx, ev)
		}
	case "source":
		switch et.Action {
		case "connected":
			return p.setStatusAndMark(ctx, ev, statusLive, false)
		case "closed":
			return p.setStatusAndMark(ctx, ev, statusOffline, true)
		}
	case "stream":
		if et.Action == "closed" {
			return p.setStatusAndMark(ctx, ev, statusOffline, true)
		}
	case "push":
		switch et.Action {
		case "connected":
			return p.setStatusAndMark(ctx, ev, statusLive, false)
		case "closed":
			return p.setStatusAndMark(ctx, ev, statusOffline, true)
		}
	case "ingest":
		switch et.Action {
		case "opened":
			// status live only, no summary publish (matches backendV2)
			return p.st.UpdateStreamStatus(ctx, ev.FlussonicStreamName, statusLive)
		case "closed":
			return p.setStatusAndMark(ctx, ev, statusOffline, true)
		}
	case "ad":
		if et.Action == "injected" {
			return p.adInjected(ctx, ev)
		}
	}
	return nil
}

// setStatusAndMark updates stream status, optionally resets viewers, then marks
// the tenant dirty for a coalesced summary rebuild.
func (p *Processor) setStatusAndMark(ctx context.Context, ev model.ProcessedEvent, status string, resetViewers bool) error {
	if err := p.st.UpdateStreamStatus(ctx, ev.FlussonicStreamName, status); err != nil {
		return err
	}
	if resetViewers {
		if err := p.state.ResetStreamViewers(ctx, ev.TenantID, ev.FlussonicStreamName); err != nil {
			return err
		}
	}
	p.coalescer.mark(ev.TenantID)
	return nil
}

func (p *Processor) playStarted(ctx context.Context, ev model.ProcessedEvent) error {
	if ev.SessionID == "" {
		return nil
	}
	ip, country := p.sessionIPCountry(ctx, ev)

	if err := p.st.SaveSessionStarted(ctx, sessionDataWith(ev, ip, country)); err != nil {
		return err
	}

	geoCountry, city := p.geoLookup(ip)
	devType := deviceType(ev.UserAgent)
	finalCountry := geoCountry
	if finalCountry == "" {
		finalCountry = country
	}
	if err := p.state.IncrementViewers(ctx, ev.TenantID, ev.FlussonicStreamName, finalCountry, city, devType); err != nil {
		return err
	}
	p.coalescer.mark(ev.TenantID)
	return nil
}

func (p *Processor) playClosed(ctx context.Context, ev model.ProcessedEvent) error {
	ip, country := p.sessionIPCountry(ctx, ev)

	var bytes *int64
	if ev.Bytes != 0 {
		b := ev.Bytes
		bytes = &b
	}
	if err := p.st.SaveSessionClose(ctx, ev.StreamID, ev.TenantID, ev.SessionID, bytes); err != nil {
		return err
	}

	geoCountry, city := p.geoLookup(ip)
	devType := deviceType(ev.UserAgent)
	finalCountry := geoCountry
	if finalCountry == "" {
		finalCountry = country
	}
	if err := p.state.DecrementViewers(ctx, ev.TenantID, ev.FlussonicStreamName, finalCountry, city, devType); err != nil {
		return err
	}
	p.coalescer.mark(ev.TenantID)
	return nil
}

func (p *Processor) adInjected(ctx context.Context, ev model.ProcessedEvent) error {
	if ev.SessionID == "" || ev.AdPath == "" {
		return nil
	}
	placement := ev.AdPlacement
	if placement == "" {
		placement = "preroll"
	}
	var dur *float64
	if ev.Duration != 0 {
		d := ev.Duration
		dur = &d
	}
	if err := p.st.SaveAdImpression(ctx, store.AdImpressionData{
		TenantID: ev.TenantID, StreamID: ev.StreamID, SessionID: ev.SessionID,
		AdPath: ev.AdPath, Placement: placement, DurationS: dur,
	}); err != nil {
		return err
	}
	if err := p.state.IncrementAdImpression(ctx, ev.TenantID, ev.FlussonicStreamName, placement); err != nil {
		return err
	}
	p.coalescer.mark(ev.TenantID)
	return nil
}

// sessionIPCountry enriches IP/country from the Flussonic session API, falling
// back to the event's own values (mirrors the getSession calls in backendV2).
func (p *Processor) sessionIPCountry(ctx context.Context, ev model.ProcessedEvent) (ip, country string) {
	ip, country = ev.IP, ev.Country
	if ev.SessionID == "" {
		return
	}
	sess, err := p.flu.GetSession(ctx, ev.SessionID)
	if err != nil {
		p.log.Debug("getSession failed", "session", ev.SessionID, "err", err)
		return
	}
	if sess != nil {
		if sess.IP != "" {
			ip = sess.IP
		}
		if sess.Country != "" {
			country = sess.Country
		}
	}
	return
}

func (p *Processor) geoLookup(ip string) (country string, city *redisstate.CityGeo) {
	if ip == "" {
		return "", nil
	}
	r := p.geo.Lookup(ip)
	if r == nil {
		return "", nil
	}
	if r.City != "" && r.CountryISO != "" {
		city = &redisstate.CityGeo{
			City: r.City, CountryCode: r.CountryISO, CountryName: r.CountryName,
			Lat: r.Lat, Lng: r.Lng,
		}
	}
	return r.CountryISO, city
}

// fetchBandwidth mirrors BandwidthFetcherService.handleTrigger: pull live stats
// from Flussonic, cache the snapshot, update the bandwidth hash, and append a
// viewer_samples row. Best-effort: failures are logged, never fail the job.
func (p *Processor) fetchBandwidth(ctx context.Context, ev model.ProcessedEvent) {
	info, err := p.flu.GetStream(ctx, ev.FlussonicStreamName)
	if err != nil {
		p.log.Debug("getStream failed", "stream", ev.FlussonicStreamName, "err", err)
		return
	}
	var stats flussonic.StreamStats
	if info.Stats != nil {
		stats = *info.Stats
	}
	title := info.Title
	if title == "" {
		title = ev.FlussonicStreamName
	}
	snapshot := map[string]any{
		"stream_id":             ev.StreamID,
		"stream_name":           title,
		"flussonic_stream_name": ev.FlussonicStreamName,
		"tenant_id":             ev.TenantID,
		"input_bitrate_kbps":    stats.InputBitrate,
		"output_bitrate_kbps":   stats.Bitrate,
		"total_bytes_in":        stats.Play.PlayBytes,
		"total_bytes_out":       stats.BytesOut,
		"online_clients":        stats.OnlineClients,
		"status":                orUnknown(stats.Status),
		"timestamp":             time.Now().UTC().Format(time.RFC3339Nano),
	}
	_ = p.state.CacheStreamSnapshot(ctx, ev.StreamID, snapshot)
	_ = p.state.UpdateStreamBandwidth(ctx, ev.TenantID, ev.FlussonicStreamName, stats.Play.PlayBytes, stats.BytesOut)
	_ = p.st.SaveViewerSample(ctx, ev.StreamID, ev.TenantID, stats.OnlineClients, stats.BytesOut)
}

// ── helpers ─────────────────────────────────────────────────────────

func sessionData(ev model.ProcessedEvent) store.SessionData {
	return store.SessionData{
		StreamID: ev.StreamID, TenantID: ev.TenantID, SessionID: ev.SessionID,
		IP: ev.IP, Country: ev.Country, City: ev.City, Protocol: ev.Protocol, UserAgent: ev.UserAgent,
	}
}

func sessionDataWith(ev model.ProcessedEvent, ip, country string) store.SessionData {
	d := sessionData(ev)
	d.IP, d.Country = ip, country
	return d
}

func deviceType(userAgent string) string {
	if userAgent == "" {
		return ""
	}
	return ua.Parse(userAgent).DeviceType
}

func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}

// eventDetails reproduces the `...eventDetails` spread: the ProcessedEvent
// minus event/time/flussonic_stream_name/stream_id/tenant_id. omitempty on the
// model means only set fields appear, matching backendV2's behaviour closely.
func eventDetails(ev model.ProcessedEvent) map[string]any {
	b, _ := json.Marshal(ev)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	delete(m, "event")
	delete(m, "time")
	delete(m, "flussonic_stream_name")
	delete(m, "stream_id")
	delete(m, "tenant_id")
	return m
}
