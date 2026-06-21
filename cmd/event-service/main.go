// Command event-service is a standalone Go service that ingests Flussonic
// event_sink webhooks, processes them through a durable asynq job queue + a
// goroutine worker pool, and writes analytics state to the Postgres/TimescaleDB
// and Redis stores it shares with backendV2. It owns the full event write-path;
// backendV2 only reads.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/streamforge/event-service/internal/config"
	"github.com/streamforge/event-service/internal/flussonic"
	"github.com/streamforge/event-service/internal/geoip"
	"github.com/streamforge/event-service/internal/ingest"
	"github.com/streamforge/event-service/internal/redisstate"
	"github.com/streamforge/event-service/internal/store"
	"github.com/streamforge/event-service/internal/streamcache"
	"github.com/streamforge/event-service/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	cfg, err := config.Load()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}

	ctx := context.Background()

	// ── Postgres pool ───────────────────────────────────────────────
	pgCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		log.Error("parse DATABASE_URL", "err", err)
		os.Exit(1)
	}
	pgCfg.MaxConns = cfg.DBPoolMaxConns
	pool, err := pgxpool.NewWithConfig(ctx, pgCfg)
	if err != nil {
		log.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Error("ping postgres", "err", err)
		os.Exit(1)
	}
	log.Info("postgres connected", "max_conns", cfg.DBPoolMaxConns)

	// ── Redis (state + pub/sub) ─────────────────────────────────────
	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		log.Error("parse REDIS_URL", "err", err)
		os.Exit(1)
	}
	rdb := redis.NewClient(redisOpts)
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Error("ping redis", "err", err)
		os.Exit(1)
	}
	log.Info("redis connected", "addr", redisOpts.Addr, "db", redisOpts.DB)

	// ── GeoIP ───────────────────────────────────────────────────────
	geo, err := geoip.Open(cfg.GeoIPCityDBPath)
	if err != nil {
		log.Error("open geoip", "err", err)
		os.Exit(1)
	}
	defer geo.Close()
	log.Info("geoip database loaded", "path", cfg.GeoIPCityDBPath, "build_epoch", geo.BuildEpoch())

	// ── Wiring ──────────────────────────────────────────────────────
	st := store.New(pool)
	cache := streamcache.New(st, 60*time.Second)
	state := redisstate.New(rdb, st.GetLiveStreamCount, cache.Name)
	flu := flussonic.New(cfg.FlussonicAPIURL, cfg.FlussonicAPIToken, cfg.FlussonicTimeout, cfg.FlussonicMaxInFlight)
	processor := worker.NewProcessor(st, state, flu, geo, cache, cfg.SummaryDebounce, log)

	if os.Getenv("CLEAR_ANALYTICS_ON_START") == "true" {
		if n, err := state.ClearAllAnalyticsKeys(ctx); err != nil {
			log.Warn("clear analytics keys", "err", err)
		} else {
			log.Info("cleared analytics keys on startup", "count", n)
		}
	}

	// ── asynq client + server ───────────────────────────────────────
	asynqRedis := asynq.RedisClientOpt{
		Addr:      redisOpts.Addr,
		Username:  redisOpts.Username,
		Password:  redisOpts.Password,
		DB:        redisOpts.DB,
		TLSConfig: redisOpts.TLSConfig,
	}
	asynqClient := asynq.NewClient(asynqRedis)
	defer asynqClient.Close()
	enqueuer := worker.NewEnqueuer(asynqClient)

	asynqSrv := asynq.NewServer(asynqRedis, asynq.Config{
		Concurrency: cfg.WorkerConcurrency,
		Logger:      asynqSlog{log},
	})
	mux := asynq.NewServeMux()
	mux.HandleFunc(worker.TaskTypeEvent, processor.HandlerFunc())
	if err := asynqSrv.Start(mux); err != nil {
		log.Error("start asynq server", "err", err)
		os.Exit(1)
	}

	// ── HTTP server ─────────────────────────────────────────────────
	h := ingest.NewHandler(cfg.EventSinkSignKey, cache, enqueuer, log)
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Get("/event-service/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	// Read-only GeoIP lookup for testing: GET /event-service/geoip?ip=8.8.8.8.
	// Returns the raw GeoLite2 record (keys match session_geoip columns). Does
	// NOT write anything.
	r.Get("/event-service/geoip", func(w http.ResponseWriter, req *http.Request) {
		ip := req.URL.Query().Get("ip")
		w.Header().Set("Content-Type", "application/json")
		if ip == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "missing ?ip= query param"})
			return
		}
		res := geo.Lookup(ip)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ip":     ip,
			"found":  res != nil,
			"result": res,
		})
	})
	// Path mirrors backendV2 exactly (global prefix api/v1 + @Controller('webhooks')
	// + 'flussonic/event-sink') so it is a drop-in for the Flussonic event_sink URL.
	r.Post("/event-service/webhooks/flussonic/event-sink", h.EventSink)

	httpSrv := &http.Server{
		Addr:              ":" + itoa(cfg.Port),
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("http listening", "port", cfg.Port)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", "err", err)
			os.Exit(1)
		}
	}()

	// ── Graceful shutdown ───────────────────────────────────────────
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	// Stop accepting new HTTP requests (no new jobs enqueued)…
	_ = httpSrv.Shutdown(shutdownCtx)
	// …then let asynq finish in-flight jobs…
	asynqSrv.Shutdown()
	// …then drain any pending coalesced summary rebuilds.
	processor.Stop()
	log.Info("bye")
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// asynqSlog adapts slog to the asynq.Logger interface.
type asynqSlog struct{ l *slog.Logger }

func (a asynqSlog) Debug(args ...any) { a.l.Debug("asynq", "msg", args) }
func (a asynqSlog) Info(args ...any)  { a.l.Info("asynq", "msg", args) }
func (a asynqSlog) Warn(args ...any)  { a.l.Warn("asynq", "msg", args) }
func (a asynqSlog) Error(args ...any) { a.l.Error("asynq", "msg", args) }
func (a asynqSlog) Fatal(args ...any) { a.l.Error("asynq-fatal", "msg", args) }
