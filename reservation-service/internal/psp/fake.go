package psp

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// FakeConfig controls how the fake provider misbehaves. Zero values mean "never".
type FakeConfig struct {
	Latency                time.Duration
	RateLimit              int     // calls per second; 0 = unlimited
	TimeoutAfterChargeRate float64 // charge is recorded, caller sees ErrTimeout
	UnavailableRate        float64 // rejected before charging with ErrUnavailable
	DeclineRate            float64 // charge declined (a business result, not an error)
}

// Fake is an in-memory PSP that honours idempotency keys the way real providers do. It keeps a
// ledger so tests can assert there is exactly one charge per key and no charge without an order.
type Fake struct {
	cfg FakeConfig

	mu      sync.Mutex
	charges map[string]*Charge // by idempotency key
	refunds map[string]string  // refund key -> charge key

	calls atomic.Int64
	seq   atomic.Int64

	rlMu     sync.Mutex
	rlWindow time.Time
	rlCount  int
}

func NewFake(cfg FakeConfig) *Fake {
	return &Fake{cfg: cfg, charges: map[string]*Charge{}, refunds: map[string]string{}}
}

// SetConfig changes behaviour at runtime, for failure injection in tests.
func (f *Fake) SetConfig(cfg FakeConfig) {
	f.mu.Lock()
	f.cfg = cfg
	f.mu.Unlock()
}

func (f *Fake) config() FakeConfig {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cfg
}

func (f *Fake) allow(limit int) bool {
	if limit <= 0 {
		return true
	}
	f.rlMu.Lock()
	defer f.rlMu.Unlock()
	now := time.Now().Truncate(time.Second)
	if !now.Equal(f.rlWindow) {
		f.rlWindow, f.rlCount = now, 0
	}
	if f.rlCount >= limit {
		return false
	}
	f.rlCount++
	return true
}

func (f *Fake) wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ErrTimeout
	}
}

func (f *Fake) Charge(ctx context.Context, key string, amountCents int64, token string) (Charge, error) {
	f.calls.Add(1)
	cfg := f.config()
	if !f.allow(cfg.RateLimit) {
		return Charge{}, ErrRateLimited
	}
	if err := f.wait(ctx, cfg.Latency); err != nil {
		return Charge{}, err
	}

	f.mu.Lock()
	if existing, ok := f.charges[key]; ok {
		c := *existing
		f.mu.Unlock()
		return c, nil // idempotent replay: same result, no second charge
	}
	if rand.Float64() < cfg.UnavailableRate {
		f.mu.Unlock()
		return Charge{}, ErrUnavailable
	}
	status := StatusSucceeded
	if token == "tok_decline" || rand.Float64() < cfg.DeclineRate {
		status = StatusDeclined
	}
	c := &Charge{
		ID:             fmt.Sprintf("ch_%d", f.seq.Add(1)),
		IdempotencyKey: key,
		AmountCents:    amountCents,
		Status:         status,
	}
	f.charges[key] = c
	result := *c
	f.mu.Unlock()

	if rand.Float64() < cfg.TimeoutAfterChargeRate {
		return Charge{}, ErrTimeout // the card WAS charged; the caller cannot tell
	}
	return result, nil
}

func (f *Fake) Lookup(ctx context.Context, key string) (Charge, bool, error) {
	if err := ctx.Err(); err != nil {
		return Charge{}, false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.charges[key]
	if !ok {
		return Charge{}, false, nil
	}
	return *c, true, nil
}

func (f *Fake) Refund(ctx context.Context, chargeKey, refundKey string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, done := f.refunds[refundKey]; done {
		return nil // idempotent
	}
	c, ok := f.charges[chargeKey]
	if !ok {
		return fmt.Errorf("psp: refund for unknown charge %s", chargeKey)
	}
	c.Refunded = true
	f.refunds[refundKey] = chargeKey
	return nil
}

// Ledger returns a copy of every charge, for assertions.
func (f *Fake) Ledger() []Charge {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Charge, 0, len(f.charges))
	for _, c := range f.charges {
		out = append(out, *c)
	}
	return out
}

// Calls returns how many Charge calls were made, including retries.
func (f *Fake) Calls() int64 { return f.calls.Load() }
