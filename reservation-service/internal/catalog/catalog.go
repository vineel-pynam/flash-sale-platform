// Package catalog keeps each drop and its units in Postgres, and loads Redis from there. Postgres is
// the truth, so Redis can be rebuilt at any time (docs/03-entities-apis-schema.md, section 5).
package catalog

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"reservation-service/internal/inventory"
)

// Drop describes one sale event.
type Drop struct {
	ID          string
	Name        string
	PriceCents  int64
	Units       int
	Buckets     int
	StartsAt    time.Time
	HoldSeconds int
	HomeRegion  string
}

// CreateDrop saves the drop and its units (U000001..UNNNNNN, spread round-robin over the buckets).
// It is safe to run again: existing units are kept, and raising Units adds the missing ones.
func CreateDrop(ctx context.Context, db *pgxpool.Pool, d Drop) error {
	if d.Units <= 0 || d.Buckets <= 0 {
		return fmt.Errorf("catalog: units and buckets must be positive")
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Setup work, not the hot path: allow more than the pool's short statement timeout.
	if _, err := tx.Exec(ctx, `SET LOCAL statement_timeout = '60s'`); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO drops (event_id, name, price_cents, total_units, starts_at, hold_seconds, home_region, state)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'LIVE')
		ON CONFLICT (event_id) DO UPDATE SET total_units = GREATEST(drops.total_units, EXCLUDED.total_units)`,
		d.ID, d.Name, d.PriceCents, d.Units, d.StartsAt, d.HoldSeconds, d.HomeRegion); err != nil {
		return fmt.Errorf("catalog: save drop: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO units (event_id, unit_id, bucket)
		SELECT $1, 'U' || CASE WHEN i < 1000000 THEN lpad(i::text, 6, '0') ELSE i::text END, (i - 1) % $3
		FROM generate_series(1, $2::int) AS i
		ON CONFLICT DO NOTHING`, d.ID, d.Units, d.Buckets); err != nil {
		return fmt.Errorf("catalog: save units: %w", err)
	}
	return tx.Commit(ctx)
}

// Rebuild replaces the drop's Redis state with what Postgres says:
//   - free units (no allocation) go back into their buckets
//   - buyers of PAID orders are marked sold, so they cannot buy again
//   - orders still being paid get their hold back as FROZEN, so the payment result completes or
//     releases it as usual
//
// Holds that were not yet paid live only in Redis and are lost; those users click Hold again.
// New Holds must be paused while this runs.
func Rebuild(ctx context.Context, db *pgxpool.Pool, inv *inventory.Store) (free int, err error) {
	tx, err := db.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL statement_timeout = '60s'`); err != nil {
		return 0, err
	}
	rows, err := tx.Query(ctx, `
		SELECT u.unit_id, u.bucket FROM units u
		WHERE u.event_id = $1
		  AND NOT EXISTS (SELECT 1 FROM allocations a WHERE a.event_id = u.event_id AND a.unit_id = u.unit_id)
		ORDER BY u.unit_id`, inv.DropID())
	if err != nil {
		return 0, fmt.Errorf("catalog: read free units: %w", err)
	}
	byBucket := make(map[int][]string, inv.Buckets())
	for rows.Next() {
		var unit string
		var bucket int
		if err := rows.Scan(&unit, &bucket); err != nil {
			rows.Close()
			return 0, err
		}
		byBucket[bucket] = append(byBucket[bucket], unit)
		free++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if err := inv.Load(ctx, byBucket); err != nil {
		return 0, fmt.Errorf("catalog: load redis: %w", err)
	}

	owned, err := tx.Query(ctx, `
		SELECT o.reservation_id, o.user_id, o.unit_id, o.state
		FROM allocations a JOIN orders o USING (order_id)
		WHERE a.event_id = $1 AND o.state IN ('PAID', 'PAYMENT_PENDING')`, inv.DropID())
	if err != nil {
		return 0, fmt.Errorf("catalog: read allocations: %w", err)
	}
	type alloc struct{ res, user, unit, state string }
	var list []alloc
	for owned.Next() {
		var a alloc
		if err := owned.Scan(&a.res, &a.user, &a.unit, &a.state); err != nil {
			owned.Close()
			return 0, err
		}
		list = append(list, a)
	}
	owned.Close()
	if err := owned.Err(); err != nil {
		return 0, err
	}
	for _, a := range list {
		if a.state == "PAID" {
			err = inv.MarkSold(ctx, a.user, a.res)
		} else {
			err = inv.RestoreFrozen(ctx, a.res, a.user, a.unit)
		}
		if err != nil {
			return 0, fmt.Errorf("catalog: restore %s: %w", a.res, err)
		}
	}
	return free, nil
}
