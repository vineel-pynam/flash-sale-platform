// Package payment turns PaymentRequested commands into PSP charges and applies the results. The
// same Resolver is used by payment workers and by the reconciler, so a result is applied the same
// way whichever path discovers it first.
package payment

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"reservation-service/internal/inventory"
	"reservation-service/internal/psp"
)

// Outcome describes what applying a result did, for logging and tests.
type Outcome string

const (
	OutcomePaid      Outcome = "PAID"
	OutcomeFailed    Outcome = "FAILED"
	OutcomeRefunded  Outcome = "REFUNDED"  // charge arrived for an order that could no longer be paid
	OutcomeDuplicate Outcome = "DUPLICATE" // already applied earlier; nothing to do
)

type Resolver struct {
	db  *pgxpool.Pool
	inv *inventory.Store
	psp *psp.Client
	log *slog.Logger
}

func NewResolver(db *pgxpool.Pool, inv *inventory.Store, client *psp.Client, log *slog.Logger) *Resolver {
	return &Resolver{db: db, inv: inv, psp: client, log: log}
}

func refundKey(reservationID string) string { return "rf-" + reservationID }

// Apply records a PSP result for a reservation. Every update is conditional on the expected
// previous state, so applying the same result twice, or a stale result, cannot move an order
// backwards. outboxID, when set, marks the command processed in the same transaction.
func (r *Resolver) Apply(ctx context.Context, reservationID string, ch psp.Charge, outboxID *int64) (Outcome, error) {
	var outcome Outcome
	var userID string

	err := pgx.BeginFunc(ctx, r.db, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT user_id FROM orders WHERE reservation_id = $1`, reservationID).Scan(&userID); err != nil {
			return fmt.Errorf("load order: %w", err)
		}

		switch ch.Status {
		case psp.StatusSucceeded:
			tag, err := tx.Exec(ctx, `
				UPDATE payments SET state = 'SUCCEEDED', psp_charge_id = $2, updated_at = now()
				WHERE reservation_id = $1 AND state = 'PENDING'`, reservationID, ch.ID)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				// Already resolved. If it was resolved as a failure (for example the reconciler gave
				// up), this charge is orphaned and must be refunded.
				var state string
				if err := tx.QueryRow(ctx, `SELECT state FROM payments WHERE reservation_id = $1`, reservationID).Scan(&state); err != nil {
					return err
				}
				if state == "DECLINED" {
					outcome = OutcomeRefunded
					return r.recordRefund(ctx, tx, reservationID, "charge succeeded after payment was closed as failed")
				}
				outcome = OutcomeDuplicate
				break
			}
			tag, err = tx.Exec(ctx, `
				UPDATE orders SET state = 'PAID', updated_at = now()
				WHERE reservation_id = $1 AND state = 'PAYMENT_PENDING'`, reservationID)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				outcome = OutcomeRefunded
				return r.recordRefund(ctx, tx, reservationID, "order no longer payable")
			}
			outcome = OutcomePaid
			if err := emit(ctx, tx, reservationID, "PaymentSucceeded", ch); err != nil {
				return err
			}

		case psp.StatusDeclined:
			tag, err := tx.Exec(ctx, `
				UPDATE payments SET state = 'DECLINED', psp_charge_id = NULLIF($2, ''), updated_at = now()
				WHERE reservation_id = $1 AND state = 'PENDING'`, reservationID, ch.ID)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				outcome = OutcomeDuplicate
				break
			}
			if _, err := tx.Exec(ctx, `
				UPDATE orders SET state = 'FAILED', updated_at = now()
				WHERE reservation_id = $1 AND state = 'PAYMENT_PENDING'`, reservationID); err != nil {
				return err
			}
			// Free the unit in the source of truth so it can be sold again.
			if _, err := tx.Exec(ctx, `DELETE FROM allocations WHERE reservation_id = $1`, reservationID); err != nil {
				return err
			}
			outcome = OutcomeFailed
			if err := emit(ctx, tx, reservationID, "PaymentFailed", ch); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown charge status %q", ch.Status)
		}

		if outboxID != nil {
			_, err := tx.Exec(ctx, `UPDATE outbox SET processed_at = now() WHERE id = $1`, *outboxID)
			return err
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	// Side effects outside the transaction. Each is idempotent; if one fails here the reconciler
	// repeats it (SyncHolds, RefundPending).
	bg := context.WithoutCancel(ctx)
	switch outcome {
	case OutcomePaid:
		if err := r.inv.Complete(bg, reservationID, userID); err != nil {
			r.log.Warn("complete hold failed; reconciler will retry", "reservation_id", reservationID, "err", err)
		}
	case OutcomeFailed:
		if err := r.inv.Release(bg, reservationID); err != nil {
			r.log.Warn("release hold failed; reconciler will retry", "reservation_id", reservationID, "err", err)
		}
	case OutcomeRefunded:
		if err := r.psp.Refund(bg, reservationID, refundKey(reservationID)); err != nil {
			r.log.Warn("refund failed; reconciler will retry", "reservation_id", reservationID, "err", err)
		} else if err := r.MarkRefundSent(bg, refundKey(reservationID)); err != nil {
			r.log.Warn("mark refund sent failed; reconciler will resend", "reservation_id", reservationID, "err", err)
		}
	}
	return outcome, nil
}

// recordRefund stores the refund intent in the same transaction, so a crash before the PSP call
// still leaves a durable record for the reconciler.
func (r *Resolver) recordRefund(ctx context.Context, tx pgx.Tx, reservationID, reason string) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO refunds (refund_id, reservation_id, reason) VALUES ($1, $2, $3)
		ON CONFLICT (refund_id) DO NOTHING`, refundKey(reservationID), reservationID, reason); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE payments SET state = 'REFUNDED', updated_at = now() WHERE reservation_id = $1`, reservationID)
	return err
}

// MarkRefundSent records that the PSP accepted a refund, so the reconciler stops resending it.
// If this write is lost the refund is simply sent again, which its idempotency key makes harmless.
func (r *Resolver) MarkRefundSent(ctx context.Context, refundID string) error {
	_, err := r.db.Exec(ctx, `UPDATE refunds SET sent_at = now() WHERE refund_id = $1 AND sent_at IS NULL`, refundID)
	return err
}

func emit(ctx context.Context, tx pgx.Tx, reservationID, eventType string, ch psp.Charge) error {
	payload, err := json.Marshal(map[string]any{"reservation_id": reservationID, "charge_id": ch.ID, "status": ch.Status})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox (aggregate_id, event_id, event_type, payload, processed_at)
		VALUES ($1, (SELECT event_id FROM orders WHERE reservation_id = $1), $2, $3, NULL)`, reservationID, eventType, payload)
	return err
}
