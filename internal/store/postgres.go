// Package store owns all Postgres/TimescaleDB writes for the event pipeline.
// Every statement mirrors backendV2's AnalyticsService / StreamEventsService
// raw SQL exactly so the shared schema stays the single source of truth and the
// dashboard read-path keeps working unchanged.
package store

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/streamforge/event-service/internal/ua"
)

type Store struct {
	pool *pgxpool.Pool

	adFileMu    sync.Mutex
	adFileCache map[string]adFileCacheEntry // "tenant:adPath" -> id
}

type adFileCacheEntry struct {
	id        *string
	expiresAt time.Time
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, adFileCache: make(map[string]adFileCacheEntry)}
}

// StreamRef is the minimal stream identity resolved from flussonic_stream_name.
type StreamRef struct {
	ID       string
	TenantID string
	Name     string
}

// ResolveStream looks up a stream by its Flussonic name. found=false when absent.
func (s *Store) ResolveStream(ctx context.Context, flussonicStreamName string) (StreamRef, bool, error) {
	var ref StreamRef
	err := s.pool.QueryRow(ctx,
		`SELECT id, tenant_id, name FROM streams WHERE flussonic_stream_name = $1`,
		flussonicStreamName,
	).Scan(&ref.ID, &ref.TenantID, &ref.Name)
	if err != nil {
		if err == pgx.ErrNoRows {
			return StreamRef{}, false, nil
		}
		return StreamRef{}, false, err
	}
	return ref, true, nil
}

type SessionData struct {
	StreamID  string
	TenantID  string
	SessionID string
	IP        string
	Country   string
	City      string
	Protocol  string
	UserAgent string
}

func nz(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// SaveSessionOpen mirrors AnalyticsService.saveSessionOpen.
func (s *Store) SaveSessionOpen(ctx context.Context, d SessionData) error {
	now := time.Now()
	p := parseUA(d.UserAgent)
	_, err := s.pool.Exec(ctx,
		`INSERT INTO session_events (time, stream_id, tenant_id, session_id, ip, country, city, protocol, user_agent, device_type, browser_name, os_name, device_brand, device_model, opened_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
		now, d.StreamID, d.TenantID, d.SessionID, nz(d.IP), nz(d.Country), nz(d.City), nz(d.Protocol), nz(d.UserAgent),
		p.deviceType, p.browser, p.os, p.brand, p.model, now,
	)
	return err
}

// SaveSessionStarted mirrors AnalyticsService.saveSessionStarted (UPDATE then
// INSERT fallback). COALESCE preserves values set by play_opened.
func (s *Store) SaveSessionStarted(ctx context.Context, d SessionData) error {
	now := time.Now()
	p := parseUA(d.UserAgent)
	tag, err := s.pool.Exec(ctx,
		`UPDATE session_events SET started_at = $1, ip = COALESCE($2, ip), country = COALESCE($3, country), city = COALESCE($4, city), protocol = COALESCE($5, protocol), user_agent = COALESCE($6, user_agent), device_type = COALESCE($7, device_type), browser_name = COALESCE($8, browser_name), os_name = COALESCE($9, os_name), device_brand = COALESCE($10, device_brand), device_model = COALESCE($11, device_model)
		 WHERE session_id = $12 AND stream_id = $13 AND tenant_id = $14`,
		now, nz(d.IP), nz(d.Country), nz(d.City), nz(d.Protocol), nz(d.UserAgent),
		p.deviceType, p.browser, p.os, p.brand, p.model,
		d.SessionID, d.StreamID, d.TenantID,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		return nil
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO session_events (time, stream_id, tenant_id, session_id, ip, country, city, protocol, user_agent, device_type, browser_name, os_name, device_brand, device_model, started_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
		now, d.StreamID, d.TenantID, d.SessionID, nz(d.IP), nz(d.Country), nz(d.City), nz(d.Protocol), nz(d.UserAgent),
		p.deviceType, p.browser, p.os, p.brand, p.model, now,
	)
	return err
}

// SaveSessionClose mirrors AnalyticsService.saveSessionClose.
func (s *Store) SaveSessionClose(ctx context.Context, streamID, tenantID, sessionID string, bytes *int64) error {
	now := time.Now()
	tag, err := s.pool.Exec(ctx,
		`UPDATE session_events
		 SET closed_at = $1,
		     bytes_sent = $2,
		     duration_s = CASE WHEN started_at IS NOT NULL THEN EXTRACT(EPOCH FROM ($1::timestamptz - started_at))::integer ELSE NULL END
		 WHERE session_id = $3 AND stream_id = $4 AND tenant_id = $5`,
		now, bytesArg(bytes), sessionID, streamID, tenantID,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		return nil
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO session_events (time, stream_id, tenant_id, session_id, closed_at, bytes_sent)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		now, streamID, tenantID, sessionID, now, bytesArg(bytes),
	)
	return err
}

func bytesArg(b *int64) any {
	if b == nil {
		return nil
	}
	return *b
}

// UpdateStreamStatus mirrors AnalyticsService.updateStreamStatus.
func (s *Store) UpdateStreamStatus(ctx context.Context, flussonicStreamName, status string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE streams SET status = $1 WHERE flussonic_stream_name = $2`,
		status, flussonicStreamName,
	)
	return err
}

// SaveViewerSample mirrors AnalyticsService.saveViewerSample.
func (s *Store) SaveViewerSample(ctx context.Context, streamID, tenantID string, viewers int, bandwidth int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO viewer_samples (time, stream_id, tenant_id, viewers, bandwidth)
		 VALUES ($1, $2, $3, $4, $5)`,
		time.Now(), streamID, tenantID, viewers, bandwidth,
	)
	return err
}

// SaveStreamEvent mirrors StreamEventsService.record DB insert. createdAt nil → now.
func (s *Store) SaveStreamEvent(ctx context.Context, streamID, tenantID, eventType string, details map[string]any, createdAt *time.Time) error {
	now := time.Now()
	if createdAt != nil {
		now = *createdAt
	}
	if details == nil {
		details = map[string]any{}
	}
	raw, err := json.Marshal(details)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO stream_events (stream_id, tenant_id, event_type, details, created_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		streamID, tenantID, eventType, string(raw), now,
	)
	return err
}

// GetLiveStreamCount mirrors AnalyticsService.getLiveStreamCount.
func (s *Store) GetLiveStreamCount(ctx context.Context, tenantID string) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*)::int FROM streams WHERE tenant_id = $1 AND status = $2`,
		tenantID, "live",
	).Scan(&count)
	return count, err
}

type AdImpressionData struct {
	TenantID  string
	StreamID  string
	SessionID string
	AdPath    string
	Placement string
	DurationS *float64
}

// SaveAdImpression mirrors AnalyticsService.saveAdImpression (+ resolveAdFileId).
func (s *Store) SaveAdImpression(ctx context.Context, d AdImpressionData) error {
	adFileID, err := s.resolveAdFileID(ctx, d.TenantID, d.AdPath)
	if err != nil {
		return err
	}
	var dur any
	if d.DurationS != nil {
		dur = *d.DurationS
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO ad_impressions (tenant_id, stream_id, session_id, ad_file_id, placement, duration_s, ad_path)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		d.TenantID, d.StreamID, d.SessionID, adFileID, d.Placement, dur, d.AdPath,
	)
	return err
}

// resolveAdFileID mirrors AnalyticsService.resolveAdFileId: exact match on
// processed_vod_subpath, then filename-only fallback; cached for 1h.
func (s *Store) resolveAdFileID(ctx context.Context, tenantID, adPath string) (any, error) {
	cacheKey := tenantID + ":" + adPath
	s.adFileMu.Lock()
	if e, ok := s.adFileCache[cacheKey]; ok && e.expiresAt.After(time.Now()) {
		s.adFileMu.Unlock()
		return ptrArg(e.id), nil
	}
	s.adFileMu.Unlock()

	id, err := s.queryAdFileID(ctx, tenantID, adPath)
	if err != nil {
		return nil, err
	}
	s.adFileMu.Lock()
	s.adFileCache[cacheKey] = adFileCacheEntry{id: id, expiresAt: time.Now().Add(time.Hour)}
	s.adFileMu.Unlock()
	return ptrArg(id), nil
}

func (s *Store) queryAdFileID(ctx context.Context, tenantID, adPath string) (*string, error) {
	var id string
	err := s.pool.QueryRow(ctx,
		`SELECT id FROM ad_files WHERE tenant_id = $1 AND processed_vod_subpath = $2 LIMIT 1`,
		tenantID, adPath,
	).Scan(&id)
	if err == nil {
		return &id, nil
	}
	if err != pgx.ErrNoRows {
		return nil, err
	}
	// filename-only fallback
	if i := lastSlash(adPath); i >= 0 {
		filename := adPath[i+1:]
		err = s.pool.QueryRow(ctx,
			`SELECT id FROM ad_files WHERE tenant_id = $1 AND processed_vod_subpath = $2 LIMIT 1`,
			tenantID, filename,
		).Scan(&id)
		if err == nil {
			return &id, nil
		}
		if err != pgx.ErrNoRows {
			return nil, err
		}
	}
	return nil, nil
}

func lastSlash(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return i
		}
	}
	return -1
}

func ptrArg(p *string) any {
	if p == nil {
		return nil
	}
	return *p
}

// parsedUA is the store-local view of ua.Parsed with NULL-able fields as `any`.
type parsedUA struct {
	deviceType any
	browser    any
	os         any
	brand      any
	model      any
}

func parseUA(s string) parsedUA {
	if s == "" {
		// Mirror TS: leave columns NULL (COALESCE handles display).
		return parsedUA{deviceType: nil, browser: nil, os: nil, brand: nil, model: nil}
	}
	p := ua.Parse(s)
	return parsedUA{
		deviceType: nz(p.DeviceType),
		browser:    nz(p.BrowserName),
		os:         nz(p.OSName),
		brand:      nz(p.DeviceBrand),
		model:      nz(p.DeviceModel),
	}
}
