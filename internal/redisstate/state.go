// Package redisstate owns all Redis reads/writes for live analytics state.
// Key formats, hash fields and the published summary JSON mirror backendV2's
// AnalyticsService exactly so the dashboard read-path + SDK SSE keep working.
package redisstate

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const maxCitiesInSummary = 50

// NameResolver returns the human-readable stream name for a flussonic stream
// name. Used to avoid the N+1 DB lookup backendV2 does inside the rebuild loop.
type NameResolver func(ctx context.Context, flussonicStreamName string) string

type State struct {
	rdb      *redis.Client
	liveFn   func(ctx context.Context, tenantID string) (int, error)
	resolver NameResolver
}

func New(rdb *redis.Client, liveFn func(ctx context.Context, tenantID string) (int, error), resolver NameResolver) *State {
	return &State{rdb: rdb, liveFn: liveFn, resolver: resolver}
}

func streamKey(tenantID, name string) string { return "sf:analytics:" + tenantID + ":" + name }
func summaryKey(tenantID string) string      { return "sf:analytics-summary:" + tenantID }

// ── City geo passed from the worker ─────────────────────────────────

type CityGeo struct {
	City        string
	CountryCode string
	CountryName string
	Lat         float64
	Lng         float64
}

// ── Viewer counters ─────────────────────────────────────────────────

func (s *State) IncrementViewers(ctx context.Context, tenantID, name, country string, city *CityGeo, deviceType string) error {
	key := streamKey(tenantID, name)
	if err := s.rdb.HIncrBy(ctx, key, "viewers", 1).Err(); err != nil {
		return err
	}
	if country != "" {
		if err := s.updateCountry(ctx, key, country, 1); err != nil {
			return err
		}
	}
	if city != nil {
		if err := s.updateCity(ctx, key, *city, 1); err != nil {
			return err
		}
	}
	if deviceType != "" {
		if err := s.updateDeviceType(ctx, key, deviceType, 1); err != nil {
			return err
		}
	}
	return nil
}

func (s *State) DecrementViewers(ctx context.Context, tenantID, name, country string, city *CityGeo, deviceType string) error {
	key := streamKey(tenantID, name)
	n, err := s.rdb.HIncrBy(ctx, key, "viewers", -1).Result()
	if err != nil {
		return err
	}
	if n < 0 {
		if err := s.rdb.HSet(ctx, key, "viewers", 0).Err(); err != nil {
			return err
		}
	}
	if country != "" {
		if err := s.updateCountry(ctx, key, country, -1); err != nil {
			return err
		}
	}
	if city != nil {
		if err := s.updateCity(ctx, key, *city, -1); err != nil {
			return err
		}
	}
	if deviceType != "" {
		if err := s.updateDeviceType(ctx, key, deviceType, -1); err != nil {
			return err
		}
	}
	return nil
}

func (s *State) updateCountry(ctx context.Context, key, country string, delta int) error {
	m := map[string]int{}
	if raw, err := s.rdb.HGet(ctx, key, "countries").Result(); err == nil && raw != "" {
		_ = json.Unmarshal([]byte(raw), &m)
	}
	m[country] += delta
	if m[country] <= 0 {
		delete(m, country)
	}
	return s.hsetJSON(ctx, key, "countries", m)
}

type cityEntry struct {
	Count       int     `json:"count"`
	CountryName string  `json:"country_name"`
	Lat         float64 `json:"lat"`
	Lng         float64 `json:"lng"`
}

func (s *State) updateCity(ctx context.Context, key string, c CityGeo, delta int) error {
	m := map[string]cityEntry{}
	if raw, err := s.rdb.HGet(ctx, key, "cities").Result(); err == nil && raw != "" {
		_ = json.Unmarshal([]byte(raw), &m)
	}
	cityKey := c.CountryCode + ":" + c.City
	if e, ok := m[cityKey]; ok {
		e.Count += delta
		if e.Count <= 0 {
			delete(m, cityKey)
		} else {
			m[cityKey] = e
		}
	} else if delta > 0 {
		m[cityKey] = cityEntry{Count: delta, CountryName: c.CountryName, Lat: c.Lat, Lng: c.Lng}
	}
	return s.hsetJSON(ctx, key, "cities", m)
}

func (s *State) updateDeviceType(ctx context.Context, key, deviceType string, delta int) error {
	m := map[string]int{}
	if raw, err := s.rdb.HGet(ctx, key, "device_types").Result(); err == nil && raw != "" {
		_ = json.Unmarshal([]byte(raw), &m)
	}
	m[deviceType] += delta
	if m[deviceType] <= 0 {
		delete(m, deviceType)
	}
	return s.hsetJSON(ctx, key, "device_types", m)
}

func (s *State) hsetJSON(ctx context.Context, key, field string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return s.rdb.HSet(ctx, key, field, string(b)).Err()
}

// ── Ad + bandwidth ──────────────────────────────────────────────────

func (s *State) IncrementAdImpression(ctx context.Context, tenantID, name, placement string) error {
	key := streamKey(tenantID, name)
	if err := s.rdb.HIncrBy(ctx, key, "ad_impressions", 1).Err(); err != nil {
		return err
	}
	switch placement {
	case "preroll":
		return s.rdb.HIncrBy(ctx, key, "ad_prerolls", 1).Err()
	case "midroll":
		return s.rdb.HIncrBy(ctx, key, "ad_midrolls", 1).Err()
	}
	return nil
}

func (s *State) UpdateStreamBandwidth(ctx context.Context, tenantID, name string, in, out int64) error {
	return s.rdb.HSet(ctx, streamKey(tenantID, name), "bandwidth_in", in, "bandwidth_out", out).Err()
}

func (s *State) ResetStreamViewers(ctx context.Context, tenantID, name string) error {
	return s.rdb.Del(ctx, streamKey(tenantID, name)).Err()
}

func (s *State) CacheStreamSnapshot(ctx context.Context, streamID string, snapshot any) error {
	b, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	return s.rdb.Set(ctx, "sf:snapshot:stream:"+streamID, b, 30*time.Second).Err()
}

// Publish marshals payload to JSON and PUBLISHes it (fire path used for SSE).
func (s *State) Publish(ctx context.Context, channel string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return s.rdb.Publish(ctx, channel, b).Err()
}

// ClearAllAnalyticsKeys removes all sf:analytics:* and sf:analytics-summary:*
// keys at startup, mirroring AnalyticsService.clearAllAnalyticsKeys.
func (s *State) ClearAllAnalyticsKeys(ctx context.Context) (int, error) {
	var total int
	for _, pattern := range []string{"sf:analytics:*", "sf:analytics-summary:*"} {
		keys, err := scanAll(ctx, s.rdb, pattern)
		if err != nil {
			return total, err
		}
		if len(keys) > 0 {
			if err := s.rdb.Del(ctx, keys...).Err(); err != nil {
				return total, err
			}
			total += len(keys)
		}
	}
	return total, nil
}

func scanAll(ctx context.Context, rdb *redis.Client, pattern string) ([]string, error) {
	var keys []string
	var cursor uint64
	for {
		batch, next, err := rdb.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, batch...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return keys, nil
}

// ── Summary types (mirror TenantAnalyticsSummary JSON) ───────────────

type StreamViewers struct {
	StreamName string `json:"streamName"`
	Viewers    int    `json:"viewers"`
	StreamKey  string `json:"streamKey"`
}
type CountryStats struct {
	Code    string `json:"code"`
	Viewers int    `json:"viewers"`
}
type CityViewers struct {
	City        string  `json:"city"`
	CountryCode string  `json:"country_code"`
	CountryName string  `json:"country_name"`
	Lat         float64 `json:"lat"`
	Lng         float64 `json:"lng"`
	Viewers     int     `json:"viewers"`
}
type DeviceStat struct {
	Label   string  `json:"label"`
	Viewers int     `json:"viewers"`
	Pct     float64 `json:"pct"`
}
type Summary struct {
	TotalViewers      int             `json:"total_viewers"`
	TotalStreamsLive  int             `json:"total_streams_live"`
	TotalBandwidthIn  int64           `json:"total_bandwidth_in"`
	TotalBandwidthOut int64           `json:"total_bandwidth_out"`
	ViewersPerStream  []StreamViewers `json:"viewers_per_stream"`
	Countries         []CountryStats  `json:"countries"`
	ViewersByCity     []CityViewers   `json:"viewers_by_city"`
	LiveDevices       []DeviceStat    `json:"live_devices"`
	AdImpressions     int             `json:"ad_impressions"`
	AdPrerolls        int             `json:"ad_prerolls"`
	AdMidrolls        int             `json:"ad_midrolls"`
}

// RebuildTenantSummary aggregates all per-stream hashes for a tenant into the
// summary hash and returns it. Mirrors AnalyticsService.rebuildTenantSummary,
// but resolves stream names via the injected resolver (cache) instead of an
// N+1 DB query per stream.
func (s *State) RebuildTenantSummary(ctx context.Context, tenantID string) (*Summary, error) {
	keys, err := scanAll(ctx, s.rdb, "sf:analytics:"+tenantID+":*")
	if err != nil {
		return nil, err
	}
	liveCount, err := s.liveFn(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	sum := &Summary{TotalStreamsLive: liveCount}
	mergedCountries := map[string]int{}
	mergedCities := map[string]cityEntry{}
	mergedDevices := map[string]int{}

	prefix := "sf:analytics:" + tenantID + ":"
	for _, key := range keys {
		data, err := s.rdb.HGetAll(ctx, key).Result()
		if err != nil || len(data) == 0 {
			continue
		}
		fsName := key[len(prefix):]
		viewers := atoiSafe(data["viewers"])
		sum.TotalViewers += viewers
		sum.TotalBandwidthIn += atoi64Safe(data["bandwidth_in"])
		sum.TotalBandwidthOut += atoi64Safe(data["bandwidth_out"])
		sum.AdImpressions += atoiSafe(data["ad_impressions"])
		sum.AdPrerolls += atoiSafe(data["ad_prerolls"])
		sum.AdMidrolls += atoiSafe(data["ad_midrolls"])

		streamName := fsName
		if s.resolver != nil {
			if n := s.resolver(ctx, fsName); n != "" {
				streamName = n
			}
		}
		sum.ViewersPerStream = append(sum.ViewersPerStream, StreamViewers{
			StreamName: streamName, Viewers: viewers, StreamKey: fsName,
		})

		if raw := data["countries"]; raw != "" {
			m := map[string]int{}
			if json.Unmarshal([]byte(raw), &m) == nil {
				for c, v := range m {
					mergedCountries[c] += v
				}
			}
		}
		if raw := data["cities"]; raw != "" {
			m := map[string]cityEntry{}
			if json.Unmarshal([]byte(raw), &m) == nil {
				for ck, ce := range m {
					if ex, ok := mergedCities[ck]; ok {
						ex.Count += ce.Count
						mergedCities[ck] = ex
					} else {
						mergedCities[ck] = ce
					}
				}
			}
		}
		if raw := data["device_types"]; raw != "" {
			m := map[string]int{}
			if json.Unmarshal([]byte(raw), &m) == nil {
				for d, v := range m {
					mergedDevices[d] += v
				}
			}
		}
	}

	for code, v := range mergedCountries {
		sum.Countries = append(sum.Countries, CountryStats{Code: code, Viewers: v})
	}
	sort.Slice(sum.Countries, func(i, j int) bool { return sum.Countries[i].Viewers > sum.Countries[j].Viewers })

	totalDeviceViewers := 0
	for _, v := range mergedDevices {
		totalDeviceViewers += v
	}
	for label, v := range mergedDevices {
		pct := 0.0
		if totalDeviceViewers > 0 {
			pct = float64(int(float64(v)/float64(totalDeviceViewers)*10000)) / 100
		}
		sum.LiveDevices = append(sum.LiveDevices, DeviceStat{Label: label, Viewers: v, Pct: pct})
	}
	sort.Slice(sum.LiveDevices, func(i, j int) bool { return sum.LiveDevices[i].Viewers > sum.LiveDevices[j].Viewers })

	for ck, ce := range mergedCities {
		code, city := splitCityKey(ck)
		sum.ViewersByCity = append(sum.ViewersByCity, CityViewers{
			City: city, CountryCode: code, CountryName: ce.CountryName,
			Lat: ce.Lat, Lng: ce.Lng, Viewers: ce.Count,
		})
	}
	sort.Slice(sum.ViewersByCity, func(i, j int) bool { return sum.ViewersByCity[i].Viewers > sum.ViewersByCity[j].Viewers })
	if len(sum.ViewersByCity) > maxCitiesInSummary {
		sum.ViewersByCity = sum.ViewersByCity[:maxCitiesInSummary]
	}

	// Persist summary hash (mirror field names/types).
	vps, _ := json.Marshal(sum.ViewersPerStream)
	countries, _ := json.Marshal(sum.Countries)
	cities, _ := json.Marshal(sum.ViewersByCity)
	devices, _ := json.Marshal(sum.LiveDevices)
	if err := s.rdb.HSet(ctx, summaryKey(tenantID),
		"total_viewers", sum.TotalViewers,
		"total_streams_live", sum.TotalStreamsLive,
		"total_bandwidth_in", sum.TotalBandwidthIn,
		"total_bandwidth_out", sum.TotalBandwidthOut,
		"viewers_per_stream", string(vps),
		"countries", string(countries),
		"viewers_by_city", string(cities),
		"live_devices", string(devices),
		"ad_impressions", sum.AdImpressions,
		"ad_prerolls", sum.AdPrerolls,
		"ad_midrolls", sum.AdMidrolls,
	).Err(); err != nil {
		return nil, err
	}
	return sum, nil
}

func splitCityKey(ck string) (code, city string) {
	for i := 0; i < len(ck); i++ {
		if ck[i] == ':' {
			return ck[:i], ck[i+1:]
		}
	}
	return ck, ""
}

func atoiSafe(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}
func atoi64Safe(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}
