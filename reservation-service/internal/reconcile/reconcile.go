// Package reconcile closes every gap the request path can leave behind: payments with no result,
// charges with no order, refunds not yet sent, and Redis holds that disagree with Postgres
// (docs/07-testing-and-observability.md, section 7).
package reconcile

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"reservation-service/internal/inventory"
	"reservation-service/internal/payment"
	"reservation-service/internal/psp"
)

type Config struct {
	Interval       time.Duration
	PendingAfter   time.Duration // query the PSP for payments pending longer than this
	GiveUpAfter    time.Duration // close as failed if the PSP has no record after this
	FrozenOrphanAt time.Duration // frozen hold with no order older than this is unfrozen
}

func DefaultConfig() Config {
	return Config{Interval: time.Minute, PendingAfter: 2 * time.Minute, GiveUpAfter: 15 * time.Minute, FrozenOrphanAt: 2 * time.Minute}
}

type Reconciler struct {
	db  *pgxpool.Pool
	inv *inventory.Store
	psp *psp.Client
	res *payment.Resolver
	cfg Config
	log *slog.Logger
}

func New(db *pgxpool.Pool, inv *inventory.Store, client *psp.Client, res *payment.Resolver, cfg Config, log *slog.Logger) *Reconciler {
	return &Reconciler{db: db, inv: inv, psp: client, res: res, cfg: cfg, log: log}
}

// Report is what one pass found and fixed. Every field is exported as a metric in production.
type Report struct {
	PendingResolved int
	PendingClosed   int
	RefundsSent     int
	HoldsSynced     int
	OrphansUnfrozen int
}

func (r *Reconciler) Run(ctx context.Context) error {
	t := time.NewTicker(r.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			rep, err := r.Once(ctx)
			if err != nil && ctx.Err() == nil {
				r.log.Error("reconcile pass failed", "err", err)
			}
			r.log.Info("reconcile pass", "report", rep)
		}
	}
}

// Once runs every job one time.
func (r *Reconciler) Once(ctx context.Context) (Report, error) {
	var rep Report
	var err error
	if rep.PendingResolved, rep.PendingClosed, err = r.ResolvePending(ctx); err != nil {
		return rep, err
	}
	if rep.RefundsSent, err = r.SendRefunds(ctx); err != nil {
		return rep, err
	}
	if rep.HoldsSynced, err = r.SyncHolds(ctx, 30*time.Minute); err != nil {
		return rep, err
	}
	if rep.OrphansUnfrozen, err = r.UnfreezeOrphans(ctx); err != nil {
		return rep, err
	}
	return rep, nil
}

// ResolvePending asks the PSP about payments that have been pending too long. Found charges are
// applied exactly as a worker would; payments the PSP has never seen are closed as failed after
// GiveUpAfter, which releases the unit.
func (r *Reconciler) ResolvePending(ctx context.Context) (resolved, closed int, err error) {
	rows, err := r.db.Query(ctx, `
		SELECT p.reservation_id, now() - p.updated_at FROM payments p
		JOIN orders o ON o.reservation_id = p.reservation_id
		WHERE o.event_id = $2 AND p.state = 'PENDING' AND p.updated_at < now() - $1::interval
		ORDER BY p.updated_at LIMIT 500`, r.cfg.PendingAfter.String(), r.inv.DropID())
	if err != nil {
		return 0, 0, err
	}
	type pending struct {
		rid string
		age time.Duration
	}
	var list []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.rid, &p.age); err != nil {
			rows.Close()
			return 0, 0, err
		}
		list = append(list, p)
	}
	rows.Close()

	for _, p := range list {
		ch, found, err := r.psp.Lookup(ctx, p.rid)
		if err != nil {
			continue // PSP unreachable; next pass
		}
		switch {
		case found:
			if _, err := r.res.Apply(ctx, p.rid, ch, nil); err == nil {
				resolved++
			}
		case p.age >= r.cfg.GiveUpAfter:
			// Close the command so no worker charges it later. If a charge still arrives, Apply
			// sees the payment is DECLINED and refunds it.
			if _, err := r.db.Exec(ctx, `UPDATE outbox SET processed_at = now()
				WHERE aggregate_id = $1 AND event_type = 'PaymentRequested' AND processed_at IS NULL`, p.rid); err != nil {
				return resolved, closed, err
			}
			if _, err := r.res.Apply(ctx, p.rid, psp.Charge{Status: psp.StatusDeclined}, nil); err == nil {
				closed++
			}
		}
	}
	return resolved, closed, nil
}

// SendRefunds sends every recorded refund the PSP has not yet accepted, oldest first. Refunds
// carry their own idempotency key, so sending one twice is harmless.
func (r *Reconciler) SendRefunds(ctx context.Context) (int, error) {
	// Refunds for this drop, plus any refund with no order at all (safe: the refund key is idempotent).
	rows, err := r.db.Query(ctx, `SELECT f.refund_id, f.reservation_id FROM refunds f
		LEFT JOIN orders o ON o.reservation_id = f.reservation_id
		WHERE f.sent_at IS NULL AND (o.event_id = $1 OR o.event_id IS NULL)
		ORDER BY f.created_at LIMIT 500`, r.inv.DropID())
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	type refund struct{ id, rid string }
	var list []refund
	for rows.Next() {
		var f refund
		if err := rows.Scan(&f.id, &f.rid); err != nil {
			return 0, err
		}
		list = append(list, f)
	}
	rows.Close()
	sent := 0
	for _, f := range list {
		if err := r.psp.Refund(ctx, f.rid, f.id); err != nil {
			continue // PSP unreachable; next pass
		}
		if err := r.res.MarkRefundSent(ctx, f.id); err != nil {
			return sent, err
		}
		sent++
	}
	return sent, nil
}

// SyncHolds repeats the Redis side effect for recently resolved orders, in case it failed after
// the database commit. Both operations are idempotent.
func (r *Reconciler) SyncHolds(ctx context.Context, window time.Duration) (int, error) {
	rows, err := r.db.Query(ctx, `
		SELECT reservation_id, user_id, state FROM orders
		WHERE event_id = $2 AND state IN ('PAID', 'FAILED') AND updated_at > now() - $1::interval`, window.String(), r.inv.DropID())
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	type o struct{ rid, user, state string }
	var list []o
	for rows.Next() {
		var x o
		if err := rows.Scan(&x.rid, &x.user, &x.state); err != nil {
			return 0, err
		}
		list = append(list, x)
	}
	rows.Close()
	n := 0
	for _, x := range list {
		var err error
		if x.state == "PAID" {
			err = r.inv.Complete(ctx, x.rid, x.user)
		} else {
			err = r.inv.Release(ctx, x.rid)
		}
		if err == nil {
			n++
		}
	}
	return n, nil
}

// UnfreezeOrphans finds holds frozen by a checkout that crashed before committing its order, and
// puts them back on a short timer so the unit is not lost.
func (r *Reconciler) UnfreezeOrphans(ctx context.Context) (int, error) {
	holds, err := r.inv.FrozenHolds(ctx, r.cfg.FrozenOrphanAt)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, h := range holds {
		var exists bool
		if err := r.db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM orders WHERE reservation_id = $1)`, h.ReservationID).Scan(&exists); err != nil {
			return n, err
		}
		if !exists {
			if err := r.inv.Unfreeze(ctx, h.ReservationID, 30*time.Second); err == nil {
				n++
			}
		}
	}
	return n, nil
}

// Invariants is the real-time correctness check behind the oversell and charged-without-order
// alerts. Every value must be zero.
type Invariants struct {
	Oversold            int64 // allocations beyond stock
	UsersWithTwoUnits   int64
	ChargedWithoutOrder int64 // successful, unrefunded PSP charges with no PAID order
	DuplicateCharges    int64 // more than one successful charge for one reservation
}

// Check computes invariants against the PSP ledger. ledger is the PSP's list of charges; with a
// real provider this comes from its reporting API.
func (r *Reconciler) Check(ctx context.Context, stock int64, ledger []psp.Charge) (Invariants, error) {
	var inv Invariants
	var allocated int64
	if err := r.db.QueryRow(ctx, `SELECT count(*) FROM allocations WHERE event_id = $1`, r.inv.DropID()).Scan(&allocated); err != nil {
		return inv, err
	}
	inv.Oversold = max(0, allocated-stock)
	if err := r.db.QueryRow(ctx, `SELECT count(*) FROM (SELECT user_id FROM allocations WHERE event_id = $1
		GROUP BY user_id HAVING count(*) > 1) d`, r.inv.DropID()).Scan(&inv.UsersWithTwoUnits); err != nil {
		return inv, err
	}

	paid := map[string]bool{}
	rows, err := r.db.Query(ctx, `SELECT reservation_id FROM orders WHERE event_id = $1 AND state = 'PAID'`, r.inv.DropID())
	if err != nil {
		return inv, err
	}
	for rows.Next() {
		var rid string
		if err := rows.Scan(&rid); err != nil {
			rows.Close()
			return inv, err
		}
		paid[rid] = true
	}
	rows.Close()

	perKey := map[string]int{}
	for _, c := range ledger {
		if c.Status != psp.StatusSucceeded || c.Refunded {
			continue
		}
		perKey[c.IdempotencyKey]++
		if !paid[c.IdempotencyKey] {
			inv.ChargedWithoutOrder++
		}
	}
	for _, n := range perKey {
		if n > 1 {
			inv.DuplicateCharges += int64(n - 1)
		}
	}
	return inv, nil
}
