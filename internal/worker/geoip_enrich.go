package worker

import (
	"context"
	"sync"
	"time"

	"github.com/streamforge/event-service/internal/geoip"
	"github.com/streamforge/event-service/internal/store"
)

const (
	ipGeoCacheMaxSize = 10_000
	ipGeoCacheTTL     = time.Hour
)

// ipGeoCache mirrors backendV2 GeoIpService.ipCache: an in-memory IP → lookup
// cache (TTL + size cap) that spares the repeated mmdb lookup for a hot IP. It
// does NOT dedup the DB write — that is handled by INSERT ... ON CONFLICT DO
// NOTHING — so a cache miss never skips persistence.
type ipGeoCache struct {
	mu sync.Mutex
	m  map[string]ipGeoEntry
}

type ipGeoEntry struct {
	res       *geoip.Result
	expiresAt time.Time
}

func newIPGeoCache() *ipGeoCache {
	return &ipGeoCache{m: make(map[string]ipGeoEntry)}
}

func (c *ipGeoCache) get(ip string) (*geoip.Result, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[ip]
	if !ok || e.expiresAt.Before(time.Now()) {
		return nil, false
	}
	return e.res, true
}

func (c *ipGeoCache) put(ip string, res *geoip.Result) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.m) >= ipGeoCacheMaxSize {
		// Evict the entry expiring soonest (oldest insertion); matches
		// backendV2's first-key (oldest) eviction closely enough.
		var oldestKey string
		var oldest time.Time
		for k, e := range c.m {
			if oldestKey == "" || e.expiresAt.Before(oldest) {
				oldestKey, oldest = k, e.expiresAt
			}
		}
		delete(c.m, oldestKey)
	}
	c.m[ip] = ipGeoEntry{res: res, expiresAt: time.Now().Add(ipGeoCacheTTL)}
}

// enrichAndSaveGeoIP mirrors backendV2 GeoIpService.enrichAndSave: look up the IP
// (cached), then persist the full enrichment to session_geoip, deduped per
// (session_id, ip). Best-effort: a lookup miss or DB error is logged and never
// fails the event job. Called at play_opened (event IP) and play_started (the
// IP resolved by the event→DB→Flussonic cascade).
func (p *Processor) enrichAndSaveGeoIP(ctx context.Context, sessionID, ip, tenantID string) {
	if ip == "" || sessionID == "" {
		return
	}
	res, ok := p.geoCache.get(ip)
	if !ok {
		res = p.geo.Lookup(ip)
		if res == nil {
			return
		}
		p.geoCache.put(ip, res)
	}
	d := store.SessionGeoIPData{
		SessionID: sessionID,
		IP:        ip,
		TenantID:  tenantID,

		ContinentCode:      res.ContinentCode,
		ContinentGeonameID: res.ContinentGeonameID,
		ContinentName:      res.ContinentName,

		CountryISO:       res.CountryISO,
		CountryGeonameID: res.CountryGeonameID,
		CountryName:      res.CountryName,

		RegisteredCountryISO:  res.RegisteredCountryISO,
		RegisteredCountryName: res.RegisteredCountryName,

		CityGeonameID: res.CityGeonameID,
		CityName:      res.City,

		Latitude:       res.Lat,
		Longitude:      res.Lng,
		AccuracyRadius: res.AccuracyRadius,
		TimeZone:       res.TimeZone,
	}
	if err := p.st.InsertSessionGeoIP(ctx, d); err != nil {
		p.log.Warn("session_geoip insert failed", "session", sessionID, "err", err)
	}
}
