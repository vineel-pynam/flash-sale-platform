package psp_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/goleak"

	"reservation-service/internal/psp"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func fastConfig() psp.ClientConfig {
	c := psp.DefaultClientConfig()
	c.AttemptTimeout = 200 * time.Millisecond
	c.BackoffBase = time.Millisecond
	c.BackoffCap = 5 * time.Millisecond
	return c
}

// A PSP timeout is ambiguous: the card may have been charged. Retrying with the same idempotency
// key must return the original charge, never create a second one (failure path A).
func TestCharge_TimeoutAfterCharge_RetryReturnsSameCharge(t *testing.T) {
	fake := psp.NewFake(psp.FakeConfig{TimeoutAfterChargeRate: 1}) // every NEW charge "times out"
	c := psp.NewClient(fake, fastConfig())

	ch, err := c.Charge(context.Background(), "res-1", 9900, "tok_ok")
	if err != nil {
		t.Fatalf("charge: %v", err)
	}
	if ch.Status != psp.StatusSucceeded {
		t.Fatalf("status = %s, want SUCCEEDED", ch.Status)
	}
	if got := len(fake.Ledger()); got != 1 {
		t.Fatalf("ledger has %d charges, want exactly 1", got)
	}
	if fake.Calls() != 2 {
		t.Fatalf("calls = %d, want 2 (one timeout, one idempotent replay)", fake.Calls())
	}
}

func TestCharge_ManyRetriesSameKey_OneCharge(t *testing.T) {
	fake := psp.NewFake(psp.FakeConfig{})
	c := psp.NewClient(fake, fastConfig())
	for i := 0; i < 20; i++ {
		if _, err := c.Charge(context.Background(), "res-1", 9900, "tok_ok"); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(fake.Ledger()); got != 1 {
		t.Fatalf("ledger has %d charges, want 1", got)
	}
}

func TestCharge_DeclineIsAResultNotAnError(t *testing.T) {
	c := psp.NewClient(psp.NewFake(psp.FakeConfig{}), fastConfig())
	ch, err := c.Charge(context.Background(), "res-1", 9900, "tok_decline")
	if err != nil || ch.Status != psp.StatusDeclined {
		t.Fatalf("got (%v, %v), want DECLINED with no error", ch.Status, err)
	}
}

// When the PSP is failing, the breaker opens and calls fail fast without reaching the PSP, which
// protects both the PSP and our workers (docs/06-scalability-and-fault-tolerance.md, section 4).
func TestCharge_BreakerOpensAndFailsFast(t *testing.T) {
	fake := psp.NewFake(psp.FakeConfig{UnavailableRate: 1})
	cfg := fastConfig()
	cfg.MaxAttempts = 1
	c := psp.NewClient(fake, cfg)

	for i := 0; i < 25; i++ {
		_, _ = c.Charge(context.Background(), "res", 1, "tok")
	}
	before := fake.Calls()
	_, err := c.Charge(context.Background(), "res", 1, "tok")
	if !errors.Is(err, psp.ErrCircuitOpen) {
		t.Fatalf("got %v, want ErrCircuitOpen", err)
	}
	if fake.Calls() != before {
		t.Fatal("open breaker still called the PSP")
	}
}

func TestCharge_RespectsCallerDeadline(t *testing.T) {
	fake := psp.NewFake(psp.FakeConfig{Latency: time.Second})
	cfg := fastConfig()
	cfg.AttemptTimeout = 50 * time.Millisecond
	c := psp.NewClient(fake, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.Charge(ctx, "res-1", 1, "tok")
	if err == nil {
		t.Fatal("expected an error")
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("took %s; retries ignored the caller's deadline", elapsed)
	}
}

func TestFullJitter_Bounds(t *testing.T) {
	for attempt := 0; attempt < 20; attempt++ {
		for i := 0; i < 1000; i++ {
			d := psp.FullJitter(attempt, 100*time.Millisecond, 2*time.Second)
			if d < 0 || d > 2*time.Second {
				t.Fatalf("attempt %d: delay %s out of [0, cap]", attempt, d)
			}
		}
	}
}
