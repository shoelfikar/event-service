# StreamForge Event Service (Go)

Standalone Go service that owns the **full Flussonic event write-path**, offloading
it from the NestJS `backendV2`. It ingests `event_sink` webhooks, processes them
through a durable **asynq** job queue + a goroutine worker pool, and writes
analytics state to the **Postgres/TimescaleDB** and **Redis** stores it shares
with backendV2. backendV2 only **reads** that state (dashboard + SDK SSE).

## Why this exists

At ~3k events/min the heavy per-event work (summary rebuilds, Flussonic calls,
DB writes) used to run in-process with the NestJS API event-loop. This service
isolates that work and bakes in the optimizations the old path lacked.

## Architecture

```
Flussonic event_sink (POST)
  → HTTP ingest: verify X-Signature → resolve stream (cache) → enqueue asynq job → 200
  → asynq queue (Redis, durable, retries)
  → worker pool (goroutines):
       session_events / stream_events / viewer_samples / ad_impressions  (Postgres)
       sf:analytics:<tenant>:<stream> hash                                (Redis)
       coalesced → sf:analytics-summary:<tenant> + publish sf:realtime-summary:<tenant>
```

### Key optimizations vs backendV2
- **Coalesced summary rebuild**: per-tenant debounce (default 1.5s) instead of a
  rebuild on every event ([internal/worker/coalescer.go](internal/worker/coalescer.go)).
- **Stream lookup cache**: `flussonic_stream_name → {id, tenant_id, name}` with a
  60s TTL — removes the per-event DB query and the N+1 inside summary rebuilds
  ([internal/streamcache/cache.go](internal/streamcache/cache.go)).
- **Capped Flussonic concurrency + timeouts** ([internal/flussonic/client.go](internal/flussonic/client.go)).

## Integration contract (shared with backendV2)

These formats are replicated from backendV2 and MUST stay in sync:

| Surface | backendV2 source |
|---|---|
| `X-Signature` = `sha1_hex(rawBody‖signKey)` | `webhooks/webhook.controller.ts` |
| `session_events` / `viewer_samples` / `ad_impressions` SQL | `analytics/analytics.service.ts` |
| `stream_events` SQL + `sf:stream-events:<tenant>` | `streams/stream-events.service.ts` |
| `sf:analytics:<tenant>:<stream>` hash + summary fields | `analytics/analytics.service.ts` |
| `sf:realtime-summary:<tenant>` payload | `analytics/analytics-event-processor.service.ts` |

**Schema ownership stays with backendV2 (TypeORM migrations).** This service treats
the schema as a contract.

### Known parity caveats
- **`sf:realtime-summary` dual-shape**: backendV2's `rebuildTenantSummary` emits
  `TenantAnalyticsSummary`, but the SDK SSE consumer reads a different
  `TenantRealtimeSummary` shape. This service replicates the `rebuildTenantSummary`
  payload; reconcile the consumer/publisher contract before cutover.
- **GeoIP**: uses `oschwald/geoip2-golang`; point `GEOIP_CITY_DB_PATH` at the SAME
  `.mmdb` backendV2 uses so the `cities` key (`<country_code>:<city>`) matches.
- **User-agent**: uses `mileusna/useragent` (close, not byte-identical to
  ua-parser-js); affects only secondary device columns.
- **`session_geoip` enrichment is ported** — `play_opened`/`play_started` write a
  full GeoLite2 row (continent, country, registered country, city, lat/lng,
  accuracy, time zone) via `InsertSessionGeoIP`, deduped per `(session_id, ip)`
  with `ON CONFLICT DO NOTHING`, behind a per-IP lookup cache (10k / 1h). Mirrors
  backendV2's `geoIpService.enrichAndSave`. The daily 365-day cleanup cron is NOT
  ported — backendV2's `@Cron` still owns `session_geoip` retention. Empty GeoIP
  subfields are stored as NULL (backendV2 occasionally stores `''`/`0`); populated
  values match.
- **Session IP/country resolution is DB-first/event-first by design** — diverges
  from backendV2. This service resolves a session's IP via a cascade (event
  payload → `session_events` row from play_opened → Flussonic `getSession` as a
  last resort), whereas backendV2 still calls `getSession` first and overrides
  the event values. The cascade avoids a Flussonic round-trip in the common case
  and stays correct in multi-Flussonic topologies (one `FLUSSONIC_API_URL`, so
  `getSession` 404s when the session lives on another node).

## Run

```bash
cp .env.example .env   # fill in shared DATABASE_URL / REDIS_URL / Flussonic / GeoIP
go run ./cmd/event-service
# point Flussonic event_sink at http://<host>:4100/webhooks/flussonic/event-sink
```

Config: see [.env.example](.env.example). Build/test:

```bash
go build ./...
go test ./...
```

## Cutover

1. Run in staging against staging DB/Redis; diff outputs vs backendV2.
2. Shadow-mode (mirror events) and compare `session_events`, `sf:analytics:*`,
   and the published summary.
3. Repoint Flussonic `event_sink` here; keep backendV2's processor as a flagged
   fallback.
4. Retire backendV2's event-sink + AnalyticsEventProcessorService +
   BandwidthFetcherService (keep `ad-backend`).
