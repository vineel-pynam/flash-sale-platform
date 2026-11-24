// Command server runs the reservation API, payment workers, hold reaper and reconciler.
//
// In production these would be separate deployments (request path vs workers, see docs/04-architecture.md);
// one binary keeps the prototype easy to run.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"

	"reservation-service/internal/catalog"
	"reservation-service/internal/checkout"
	"reservation-service/internal/config"
	"reservation-service/internal/httpapi"
	"reservation-service/internal/inventory"
	"reservation-service/internal/payment"
	"reservation-service/internal/psp"
	"reservation-service/internal/reconcile"
)

func main() {
	seed := flag.Bool("seed", false, "create the drop in Postgres if missing, then load Redis from Postgres on start")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(log, *seed); err != nil {
		log.Error("server exited with error", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, seed bool) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	// Root context is cancelled on SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- Redis: small timeouts; the reservation budget for Redis is 15 ms p99 (docs/02-scale-and-numbers.md, point 11).
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		PoolSize:     cfg.RedisPoolSize,
		DialTimeout:  500 * time.Millisecond,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: 100 * time.Millisecond,
		PoolTimeout:  50 * time.Millisecond, // fail fast instead of queueing on an exhausted pool
		MaxRetries:   1,                     // one retry on connection errors only (docs/06-scalability-and-fault-tolerance.md, section 4)
	})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return err
	}

	// --- Postgres: bounded pools. Sum of (HPA max replicas x pool size) must stay within 80% of
	// max_connections (docs/02-scale-and-numbers.md, point 9), so autoscaling can never exhaust the database.
	// Two pools act as a bulkhead: a flood of Pay requests cannot take the connections the payment
	// worker needs to finish payments (in production these are separate deployments anyway).
	db, err := newPool(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer db.Close()
	workerDB, err := newPool(ctx, cfg.DatabaseURL, cfg.DBWorkerConns)
	if err != nil {
		return err
	}
	defer workerDB.Close()
	if err := db.Ping(ctx); err != nil {
		return err
	}

	inv := inventory.NewStore(rdb, inventory.Options{DropID: cfg.DropID, Buckets: cfg.DropBuckets, HoldTTL: cfg.HoldTTL})
	if seed {
		// Save the drop and its units in Postgres (the truth), then load Redis from Postgres.
		if err := catalog.CreateDrop(ctx, workerDB, catalog.Drop{
			ID: cfg.DropID, Name: cfg.DropID, PriceCents: cfg.PriceCents, Units: cfg.DropUnits,
			Buckets: cfg.DropBuckets, StartsAt: time.Now(), HoldSeconds: int(cfg.HoldTTL.Seconds()), HomeRegion: "local",
		}); err != nil {
			return err
		}
		free, err := catalog.Rebuild(ctx, workerDB, inv)
		if err != nil {
			return err
		}
		log.Info("inventory loaded from postgres", "drop", cfg.DropID, "units", cfg.DropUnits, "free", free, "buckets", cfg.DropBuckets)
	}

	// The prototype uses an in-process fake PSP. A real provider implements psp.Provider.
	provider := psp.NewFake(psp.FakeConfig{
		Latency: cfg.PSPLatency, RateLimit: cfg.PSPRateLimit,
		TimeoutAfterChargeRate: cfg.PSPTimeoutAfterChargeRate, DeclineRate: cfg.PSPDeclineRate,
	})
	pcCfg := psp.DefaultClientConfig()
	pcCfg.MaxAttempts = 2 // the outbox reschedules further retries with backoff
	pspClient := psp.NewClient(provider, pcCfg)

	co := checkout.NewService(db, inv, cfg.PriceCents)
	resolver := payment.NewResolver(workerDB, inv, pspClient, log)
	wcfg := payment.DefaultWorkerConfig()
	wcfg.DropID = cfg.DropID
	worker := payment.NewWorker(workerDB, pspClient, resolver, wcfg, log)
	rec := reconcile.New(workerDB, inv, pspClient, resolver, reconcile.DefaultConfig(), log)

	api := httpapi.New(httpapi.Config{
		DropID: cfg.DropID, AdmissionKey: cfg.AdmissionTokenKey,
		MaxInflightReserve: cfg.MaxInflightReserve, MaxInflightCheckout: cfg.MaxInflightCheckout,
	}, rdb, inv, co)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Background loops share one lifetime; they stop when workCtx is cancelled during shutdown,
	// after the HTTP server has drained.
	workCtx, stopWork := context.WithCancel(context.WithoutCancel(ctx))
	defer stopWork()
	g, gctx := errgroup.WithContext(workCtx)
	g.Go(func() error { return worker.Run(gctx) })
	g.Go(func() error { return rec.Run(gctx) })
	g.Go(func() error { return reapLoop(gctx, inv, log) })

	serveErr := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
		close(serveErr)
	}()

	select {
	case err := <-serveErr:
		stopWork()
		_ = g.Wait()
		return err
	case <-ctx.Done():
	}

	// Graceful shutdown (docs/06-scalability-and-fault-tolerance.md, section 8):
	// 1. fail readiness so the load balancer stops sending new requests,
	// 2. wait for it to notice,
	// 3. stop accepting and drain in-flight requests (reservations and checkouts complete),
	// 4. stop workers: they finish the charge in hand and stop leasing,
	// 5. close pools (deferred).
	log.Info("shutdown started")
	api.SetReady(false)
	time.Sleep(cfg.ShutdownDrainDelay)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http drain incomplete", "err", err)
	}
	stopWork()
	if err := g.Wait(); err != nil {
		log.Error("worker shutdown", "err", err)
	}
	log.Info("shutdown complete")
	return nil
}

// reapLoop expires holds every second. In production two instances run with a lease; if both stop,
// holds simply last longer (lost sales, never an oversell) and the reaper-lag alert fires.
func reapLoop(ctx context.Context, inv *inventory.Store, log *slog.Logger) error {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			n, err := inv.ExpireDue(ctx, 500)
			if err != nil && ctx.Err() == nil {
				log.Error("reaper", "err", err)
			}
			if n > 0 {
				log.Info("holds expired", "count", n)
			}
		}
	}
}

// newPool opens a bounded Postgres pool with a short statement timeout (fail fast, never queue).
func newPool(ctx context.Context, url string, maxConns int32) (*pgxpool.Pool, error) {
	pcfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	pcfg.MaxConns = maxConns
	pcfg.MinConns = min(2, maxConns)
	pcfg.MaxConnIdleTime = 5 * time.Minute
	pcfg.ConnConfig.RuntimeParams["statement_timeout"] = "300"
	db, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}
