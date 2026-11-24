// Package checkout records intent before any money moves: it freezes the hold, then writes the
// order, the allocation (oversell backstop), the payment row and a PaymentRequested outbox event
// in ONE Postgres transaction (docs/adr/0008).
package checkout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"reservation-service/internal/inventory"
)

var (
	ErrNotFound      = errors.New("checkout: reservation not found")
	ErrExpired       = errors.New("checkout: reservation expired")
	ErrForbidden     = errors.New("checkout: reservation belongs to another user")
	ErrUnitTaken     = errors.New("checkout: unit already allocated to another order")
	ErrAlreadyBought = errors.New("checkout: user already has an allocation in this drop")
)

// Order is the durable record of a purchase attempt.
type Order struct {
	OrderID       uuid.UUID `json:"order_id"`
	ReservationID string    `json:"reservation_id"`
	EventID       string    `json:"event_id"`
	UserID        string    `json:"user_id"`
	UnitID        string    `json:"unit_id"`
	AmountCents   int64     `json:"amount_cents"`
	State         string    `json:"state"`
}

// PaymentRequested is the outbox payload consumed by payment workers.
type PaymentRequested struct {
	OrderID       uuid.UUID `json:"order_id"`
	ReservationID string    `json:"reservation_id"`
	AmountCents   int64     `json:"amount_cents"`
}

type Service struct {
	db         *pgxpool.Pool
	inv        *inventory.Store
	priceCents int64
	// How long an unfrozen hold gets back after a checkout that failed before charging.
	retryWindow time.Duration
}

func NewService(db *pgxpool.Pool, inv *inventory.Store, priceCents int64) *Service {
	return &Service{db: db, inv: inv, priceCents: priceCents, retryWindow: 60 * time.Second}
}

// Submit starts payment for a reservation. It is idempotent: submitting the same reservation
// again returns the existing order.
func (s *Service) Submit(ctx context.Context, reservationID, userID, paymentToken string) (Order, error) {
	if o, err := s.Get(ctx, reservationID); err == nil {
		if o.UserID != userID {
			return Order{}, ErrForbidden
		}
		return o, nil
	} else if !errors.Is(err, ErrNotFound) {
		return Order{}, err
	}

	hold, _, err := s.inv.Get(ctx, reservationID)
	switch {
	case errors.Is(err, inventory.ErrNotFound):
		return Order{}, ErrNotFound
	case err != nil:
		return Order{}, err
	case hold.UserID != userID:
		return Order{}, ErrForbidden
	}

	// 1. Freeze first: rejects an expired hold before anything is written, and stops the reaper
	//    from releasing the unit while payment is in flight.
	hold, err = s.inv.Freeze(ctx, reservationID)
	switch {
	case errors.Is(err, inventory.ErrExpired):
		return Order{}, ErrExpired
	case errors.Is(err, inventory.ErrNotFound):
		return Order{}, ErrNotFound
	case err != nil:
		return Order{}, err
	}

	// 2. One transaction: order + allocation + payment + outbox.
	o := Order{
		OrderID: uuid.Must(uuid.NewV7()), ReservationID: reservationID, EventID: s.inv.DropID(),
		UserID: userID, UnitID: hold.UnitID, AmountCents: s.priceCents, State: "PAYMENT_PENDING",
	}
	err = s.insert(ctx, o, paymentToken)
	if err == nil {
		return o, nil
	}

	// 3. Nothing was charged. Decide what happens to the hold.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		switch pgErr.ConstraintName {
		case "allocations_pkey":
			// Redis handed out a unit Postgres already gave to someone else (async failover).
			// Do not return the unit to Redis, or the duplicate would happen again.
			_ = s.inv.Discard(ctx, reservationID, userID)
			return Order{}, ErrUnitTaken
		case "allocations_event_id_user_id_key":
			_ = s.inv.Release(ctx, reservationID)
			return Order{}, ErrAlreadyBought
		case "orders_reservation_id_key", "payments_pkey", "allocations_reservation_id_key":
			// A concurrent submit for the same reservation won the race; return its order.
			return s.Get(ctx, reservationID)
		}
	}
	// Transient failure (timeout, pool exhausted): give the user a short window to retry.
	_ = s.inv.Unfreeze(context.WithoutCancel(ctx), reservationID, s.retryWindow)
	return Order{}, fmt.Errorf("checkout: %w", err)
}

func (s *Service) insert(ctx context.Context, o Order, paymentToken string) error {
	payload, err := json.Marshal(PaymentRequested{OrderID: o.OrderID, ReservationID: o.ReservationID, AmountCents: o.AmountCents})
	if err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO orders (order_id, reservation_id, event_id, user_id, unit_id, amount_cents, state)
			VALUES ($1, $2, $3, $4, $5, $6, 'PAYMENT_PENDING')`,
			o.OrderID, o.ReservationID, o.EventID, o.UserID, o.UnitID, o.AmountCents); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO allocations (event_id, unit_id, user_id, reservation_id, order_id)
			VALUES ($1, $2, $3, $4, $5)`,
			o.EventID, o.UnitID, o.UserID, o.ReservationID, o.OrderID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO payments (reservation_id, order_id, amount_cents, payment_token, state)
			VALUES ($1, $2, $3, $4, 'PENDING')`,
			o.ReservationID, o.OrderID, o.AmountCents, paymentToken); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO outbox (aggregate_id, event_id, event_type, payload) VALUES ($1, $2, 'PaymentRequested', $3)`,
			o.ReservationID, o.EventID, payload)
		return err
	})
}

// Get returns the order for a reservation.
func (s *Service) Get(ctx context.Context, reservationID string) (Order, error) {
	var o Order
	err := s.db.QueryRow(ctx, `
		SELECT order_id, reservation_id, event_id, user_id, unit_id, amount_cents, state
		FROM orders WHERE reservation_id = $1`, reservationID).
		Scan(&o.OrderID, &o.ReservationID, &o.EventID, &o.UserID, &o.UnitID, &o.AmountCents, &o.State)
	if errors.Is(err, pgx.ErrNoRows) {
		return Order{}, ErrNotFound
	}
	return o, err
}
