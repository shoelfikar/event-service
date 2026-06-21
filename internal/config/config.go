// Package config loads runtime configuration from environment variables.
//
// The event-service shares its data stores (Postgres/TimescaleDB + Redis) with
// the NestJS backendV2, so the connection-related env vars MUST match that
// service's values in any given environment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	// HTTP
	Port int

	// Shared data stores (must match backendV2)
	DatabaseURL string
	RedisURL    string

	// Flussonic API (for bandwidth/session enrichment)
	FlussonicAPIURL   string
	FlussonicAPIToken string

	// Signature verification for the Flussonic event_sink endpoint.
	// Empty disables verification (mirrors backendV2 behaviour).
	EventSinkSignKey string

	// GeoIP GeoLite2-City database path (.mmdb)
	GeoIPCityDBPath string

	// Tuning
	DBPoolMaxConns       int32
	WorkerConcurrency    int
	SummaryDebounce      time.Duration // coalescing window per tenant
	FlussonicTimeout     time.Duration
	FlussonicMaxInFlight int

	NodeEnv string
}

// Load reads configuration from the environment, applying defaults and
// validating required keys. It returns an error listing every missing required
// variable so misconfiguration fails fast at startup.
func Load() (*Config, error) {
	loadDotenv()

	cfg := &Config{
		Port:                 envInt("PORT", 4100),
		DatabaseURL:          os.Getenv("DATABASE_URL"),
		RedisURL:             os.Getenv("REDIS_URL"),
		FlussonicAPIURL:      strings.TrimRight(os.Getenv("FLUSSONIC_API_URL"), "/"),
		FlussonicAPIToken:    os.Getenv("FLUSSONIC_API_TOKEN"),
		EventSinkSignKey:     os.Getenv("FLUSSONIC_EVENT_SINK_SIGN_KEY"),
		GeoIPCityDBPath:      os.Getenv("GEOIP_CITY_DB_PATH"),
		DBPoolMaxConns:       int32(envInt("DB_POOL_SIZE", 20)),
		WorkerConcurrency:    envInt("WORKER_CONCURRENCY", 16),
		SummaryDebounce:      time.Duration(envInt("SUMMARY_DEBOUNCE_MS", 1500)) * time.Millisecond,
		FlussonicTimeout:     time.Duration(envInt("FLUSSONIC_TIMEOUT_MS", 4000)) * time.Millisecond,
		FlussonicMaxInFlight: envInt("FLUSSONIC_MAX_INFLIGHT", 8),
		NodeEnv:              envStr("NODE_ENV", "development"),
	}

	var missing []string
	for k, v := range map[string]string{
		"DATABASE_URL":        cfg.DatabaseURL,
		"REDIS_URL":           cfg.RedisURL,
		"FLUSSONIC_API_URL":   cfg.FlussonicAPIURL,
		"FLUSSONIC_API_TOKEN": cfg.FlussonicAPIToken,
		"GEOIP_CITY_DB_PATH":  cfg.GeoIPCityDBPath,
	} {
		if v == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}

	// backendV2 stores REDIS_URL scheme-less (e.g. "localhost:6379"); go-redis
	// ParseURL requires a scheme. Normalize so the value can be copied verbatim.
	if !strings.Contains(cfg.RedisURL, "://") {
		cfg.RedisURL = "redis://" + cfg.RedisURL
	}

	return cfg, nil
}

// loadDotenv loads environment variables from one or more .env files when
// present. Precedence is first-wins (godotenv never overrides an already-set
// variable), so the order is: real process env > earlier file > later file.
//
// ENV_FILE may be a comma-separated list to control the files explicitly. When
// unset it defaults to the event-service's own ".env" (overlay: PORT, GeoIP,
// tuning) followed by "../backendV2/.env" (shared DATABASE_URL/REDIS_URL/
// Flussonic), so running from the service dir reuses backendV2's values without
// duplicating secrets.
func loadDotenv() {
	var paths []string
	if v := os.Getenv("ENV_FILE"); v != "" {
		paths = strings.Split(v, ",")
	} else {
		paths = []string{".env"}
	}
	for _, p := range paths {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, err := os.Stat(p); err == nil {
			_ = godotenv.Load(p)
		}
	}
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
