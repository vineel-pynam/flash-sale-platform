// Command verify checks the correctness invariants after a load test. A load test passes only if
// performance AND correctness hold (docs/07-testing-and-observability.md, section 1).
//
//	go run ./cmd/verify            exits non-zero if any invariant is violated
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"reservation-service/internal/config"
	"reservation-service/internal/inventory"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer db.Close()
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()
	inv := inventory.NewStore(rdb, inventory.Options{DropID: cfg.DropID, Buckets: cfg.DropBuckets})

	q := func(sql string) int64 {
		var n int64
		if err := db.QueryRow(ctx, sql, cfg.DropID).Scan(&n); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		return n
	}
	allocations := q(`SELECT count(*) FROM allocations WHERE event_id = $1`)
	twoUnits := q(`SELECT count(*) FROM (SELECT user_id FROM allocations WHERE event_id = $1 GROUP BY user_id HAVING count(*) > 1) d`)
	paid := q(`SELECT count(*) FROM orders WHERE event_id = $1 AND state = 'PAID'`)
	pending := q(`SELECT count(*) FROM orders WHERE event_id = $1 AND state = 'PAYMENT_PENDING'`)
	failed := q(`SELECT count(*) FROM orders WHERE event_id = $1 AND state = 'FAILED'`)
	orphanAlloc := q(`SELECT count(*) FROM allocations a JOIN orders o USING (order_id)
		WHERE a.event_id = $1 AND o.state NOT IN ('PAID', 'PAYMENT_PENDING')`)
	available, err := inv.Available(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	// Stock comes from the drop's row in Postgres (the truth); DROP_UNITS is only a fallback.
	stock := int64(cfg.DropUnits)
	if err := db.QueryRow(ctx, `SELECT total_units FROM drops WHERE event_id = $1`, cfg.DropID).Scan(&stock); err != nil {
		stock = int64(cfg.DropUnits)
	}
	oversold := max(0, allocations-stock)
	fmt.Printf("stock=%d allocations=%d paid=%d pending=%d failed=%d redis_available=%d\n",
		stock, allocations, paid, pending, failed, available)
	fmt.Printf("oversold=%d users_with_two_units=%d allocations_for_failed_orders=%d\n",
		oversold, twoUnits, orphanAlloc)

	if oversold != 0 || twoUnits != 0 || orphanAlloc != 0 || allocations+available > stock {
		fmt.Println("RESULT: FAIL")
		os.Exit(1)
	}
	fmt.Println("RESULT: PASS")
}
