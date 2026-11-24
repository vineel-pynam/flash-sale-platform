// Package psp defines the payment provider contract, a fake provider for tests and local runs, and
// a resilient client (timeouts, jittered retries, circuit breaker) used by payment workers.
package psp

import (
	"context"
	"errors"
)

type Status string

const (
	StatusSucceeded Status = "SUCCEEDED"
	StatusDeclined  Status = "DECLINED"
)

// Charge is the provider's record of a payment, keyed by our idempotency key.
type Charge struct {
	ID             string
	IdempotencyKey string
	AmountCents    int64
	Status         Status
	Refunded       bool
}

var (
	// ErrTimeout is ambiguous: the provider may or may not have charged the card.
	ErrTimeout = errors.New("psp: timeout")
	// ErrUnavailable means the provider rejected the call before charging (5xx).
	ErrUnavailable = errors.New("psp: unavailable")
	// ErrRateLimited means the provider rejected the call before charging (429).
	ErrRateLimited = errors.New("psp: rate limited")
)

// Provider is the subset of a PSP API the platform depends on. Every mutating call takes an
// idempotency key: repeating a call with the same key returns the original result.
type Provider interface {
	Charge(ctx context.Context, idempotencyKey string, amountCents int64, paymentToken string) (Charge, error)
	Lookup(ctx context.Context, idempotencyKey string) (Charge, bool, error)
	Refund(ctx context.Context, chargeKey, refundKey string) error
}

// Retryable reports whether a failed call may be retried with the same idempotency key.
func Retryable(err error) bool {
	return errors.Is(err, ErrTimeout) || errors.Is(err, ErrUnavailable) ||
		errors.Is(err, ErrRateLimited) || errors.Is(err, context.DeadlineExceeded)
}
