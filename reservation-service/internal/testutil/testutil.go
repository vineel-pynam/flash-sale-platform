// Package testutil connects integration tests to the Redis and Postgres started by
// docker-compose.yml. Tests skip when REDIS_ADDR / DATABASE_URL are not set, so `go test ./...`
// still runs the unit tests on a machine without Docker.
package testutil

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// Redis returns a client with a pool large enough for high-concurrency tests.
func Redis(t testing.TB, poolSize int) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set; start dependencies with `docker compose up -d` (see README)")
	}
	rdb := redis.NewClient(&redis.Options{
		Addr: addr, PoolSize: poolSize, MinIdleConns: min(poolSize, 64),
		ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second, PoolTimeout: 5 * time.Second,
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// Postgres returns a pool connected to a clean schema: all tables are truncated.
func Postgres(t testing.TB, maxConns int32) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; start dependencies with `docker compose up -d` (see README)")
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse DATABASE_URL: %v", err)
	}
	cfg.MaxConns = maxConns
	db, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(db.Close)
	if _, err := db.Exec(context.Background(),
		`TRUNCATE refunds, outbox, payments, allocations, orders, units, drops, processed_messages`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return db
}

// DropID returns a unique drop name so tests never share Redis keys.
func DropID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, rand.Int64N(1_000_000_000))
}
