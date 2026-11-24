package psp

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/sony/gobreaker/v2"
)

// ClientConfig bounds every call to the provider.
type ClientConfig struct {
	AttemptTimeout time.Duration // per-call timeout (2.5 s, docs/06-scalability-and-fault-tolerance.md, section 4)
	MaxAttempts    int           // including the first
	BackoffBase    time.Duration
	BackoffCap     time.Duration
	Breaker        gobreaker.Settings
}

func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		AttemptTimeout: 2500 * time.Millisecond,
		MaxAttempts:    5,
		BackoffBase:    250 * time.Millisecond,
		BackoffCap:     15 * time.Second,
		Breaker: gobreaker.Settings{
			Name:        "psp",
			MaxRequests: 5,                // probes allowed in half-open
			Interval:    10 * time.Second, // rolling window for counts
			Timeout:     10 * time.Second, // open -> half-open
			ReadyToTrip: func(c gobreaker.Counts) bool {
				return c.Requests >= 20 && float64(c.TotalFailures)/float64(c.Requests) >= 0.5
			},
		},
	}
}

// ErrCircuitOpen is returned without calling the provider while the breaker is open.
var ErrCircuitOpen = gobreaker.ErrOpenState

// Client wraps a Provider with a per-attempt timeout, retries with exponential backoff and full
// jitter, and a circuit breaker. Every retry reuses the same idempotency key, so a retry can never
// create a second charge.
type Client struct {
	p   Provider
	cfg ClientConfig
	cb  *gobreaker.CircuitBreaker[Charge]
}

func NewClient(p Provider, cfg ClientConfig) *Client {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 1
	}
	return &Client{p: p, cfg: cfg, cb: gobreaker.NewCircuitBreaker[Charge](cfg.Breaker)}
}

// Charge returns the provider's result. A declined card is a successful call with
// Status == StatusDeclined. Errors mean the outcome is unknown or the call was not made.
func (c *Client) Charge(ctx context.Context, key string, amountCents int64, token string) (Charge, error) {
	var lastErr error
	for attempt := 0; attempt < c.cfg.MaxAttempts; attempt++ {
		ch, err := c.cb.Execute(func() (Charge, error) {
			actx, cancel := context.WithTimeout(ctx, c.cfg.AttemptTimeout)
			defer cancel()
			return c.p.Charge(actx, key, amountCents, token)
		})
		if err == nil {
			return ch, nil
		}
		lastErr = err
		if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
			return Charge{}, ErrCircuitOpen // do not hammer a failing provider; caller reschedules
		}
		if !Retryable(err) || ctx.Err() != nil {
			return Charge{}, err
		}
		if attempt+1 == c.cfg.MaxAttempts {
			break
		}
		delay := FullJitter(attempt, c.cfg.BackoffBase, c.cfg.BackoffCap)
		// Respect the caller's deadline: do not start an attempt that cannot finish.
		if dl, ok := ctx.Deadline(); ok && time.Until(dl) < delay+c.cfg.AttemptTimeout {
			return Charge{}, err
		}
		if err := sleep(ctx, delay); err != nil {
			return Charge{}, err
		}
	}
	return Charge{}, lastErr
}

// Lookup queries a charge by idempotency key, bounded by the attempt timeout.
func (c *Client) Lookup(ctx context.Context, key string) (Charge, bool, error) {
	actx, cancel := context.WithTimeout(ctx, c.cfg.AttemptTimeout)
	defer cancel()
	return c.p.Lookup(actx, key)
}

// Refund refunds a charge with its own idempotency key.
func (c *Client) Refund(ctx context.Context, chargeKey, refundKey string) error {
	actx, cancel := context.WithTimeout(ctx, c.cfg.AttemptTimeout)
	defer cancel()
	return c.p.Refund(actx, chargeKey, refundKey)
}

// State exposes the breaker state for metrics and the admission controller.
func (c *Client) State() gobreaker.State { return c.cb.State() }

// FullJitter returns random(0, min(cap, base * 2^attempt)). Full jitter spreads retries evenly so
// rejected callers never come back in synchronised waves (docs/06-scalability-and-fault-tolerance.md, section 4).
func FullJitter(attempt int, base, cap time.Duration) time.Duration {
	d := base << attempt
	if d <= 0 || d > cap {
		d = cap
	}
	return time.Duration(rand.Int64N(int64(d) + 1))
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
