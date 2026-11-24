// Package config loads service configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr          string
	RedisAddr         string
	RedisPoolSize     int
	DatabaseURL       string
	DBMaxConns        int32
	DBWorkerConns     int32
	AdmissionTokenKey []byte

	DropID      string
	DropUnits   int
	DropBuckets int
	HoldTTL     time.Duration
	PriceCents  int64

	MaxInflightReserve  int
	MaxInflightCheckout int

	PSPRateLimit              int
	PSPLatency                time.Duration
	PSPTimeoutAfterChargeRate float64
	PSPDeclineRate            float64
	ShutdownDrainDelay        time.Duration
	ShutdownTimeout           time.Duration
}

// Load reads configuration from the environment. Required values without a safe default return an
// error rather than silently falling back.
func Load() (Config, error) {
	c := Config{
		HTTPAddr:                  env("HTTP_ADDR", ":8080"),
		RedisAddr:                 env("REDIS_ADDR", "127.0.0.1:6379"),
		RedisPoolSize:             envInt("REDIS_POOL_SIZE", 64),
		DatabaseURL:               os.Getenv("DATABASE_URL"),
		DBMaxConns:                int32(envInt("DB_MAX_CONNS", 10)),
		DBWorkerConns:             int32(envInt("DB_WORKER_CONNS", 6)),
		AdmissionTokenKey:         []byte(os.Getenv("ADMISSION_TOKEN_KEY")),
		DropID:                    env("DROP_ID", "drop-1"),
		DropUnits:                 envInt("DROP_UNITS", 50_000),
		DropBuckets:               envInt("DROP_BUCKETS", 50),
		HoldTTL:                   envDuration("HOLD_TTL", 3*time.Minute),
		PriceCents:                int64(envInt("PRICE_CENTS", 9900)),
		MaxInflightReserve:        envInt("MAX_INFLIGHT_RESERVE", 1000),
		MaxInflightCheckout:       envInt("MAX_INFLIGHT_CHECKOUT", 200),
		PSPRateLimit:              envInt("PSP_RATE_LIMIT", 1000),
		PSPLatency:                envDuration("PSP_LATENCY", 50*time.Millisecond),
		PSPTimeoutAfterChargeRate: envFloat("PSP_TIMEOUT_AFTER_CHARGE_RATE", 0.01),
		PSPDeclineRate:            envFloat("PSP_DECLINE_RATE", 0.05),
		ShutdownDrainDelay:        envDuration("SHUTDOWN_DRAIN_DELAY", 5*time.Second),
		ShutdownTimeout:           envDuration("SHUTDOWN_TIMEOUT", 30*time.Second),
	}
	if c.DatabaseURL == "" {
		return c, fmt.Errorf("DATABASE_URL is required")
	}
	if len(c.AdmissionTokenKey) < 16 || string(c.AdmissionTokenKey) == "replace-with-random-hex" {
		return c, fmt.Errorf("ADMISSION_TOKEN_KEY must be a random value of at least 16 bytes (see .env.example)")
	}
	if c.DropBuckets <= 0 || c.DropUnits <= 0 {
		return c, fmt.Errorf("DROP_UNITS and DROP_BUCKETS must be positive")
	}
	return c, nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return def
}

func envFloat(k string, def float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(k), 64); err == nil {
		return v
	}
	return def
}

func envDuration(k string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(k)); err == nil {
		return v
	}
	return def
}
