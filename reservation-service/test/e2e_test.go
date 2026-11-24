// Package e2e runs the whole money path against real Redis and Postgres with an unreliable PSP,
// and checks that every failure converges to a consistent state.
package e2e

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker/v2"

	"reservation-service/internal/catalog"
	"reservation-service/internal/checkout"
	"reservation-service/internal/inventory"
	"reservation-service/internal/payment"
	"reservation-service/internal/psp"
	"reservation-service/internal/reconcile"
	"reservation-service/internal/testutil"
)

type rig struct {
	rdb      *redis.Client
	db       *pgxpool.Pool
	inv      *inventory.Store
	fake     *psp.Fake
	client   *psp.Client
	checkout *checkout.Service
	resolver *payment.Resolver
	rec      *reconcile.Reconciler
	log      *slog.Logger
}

func newRig(t *testing.T, units, buckets int, fakeCfg psp.FakeConfig) *rig {
	t.Helper()
	r := &rig{
		rdb: testutil.Redis(t, 256),
		db:  testutil.Postgres(t, 40),
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	r.inv = inventory.NewStore(r.rdb, inventory.Options{DropID: testutil.DropID("e2e"), Buckets: buckets, HoldTTL: 10 * time.Minute})
	t.Cleanup(func() { _ = r.inv.Reset(context.Background()) })
	if err := catalog.CreateDrop(context.Background(), r.db, catalog.Drop{
		ID: r.inv.DropID(), Name: "e2e", PriceCents: 9900, Units: units, Buckets: buckets,
		StartsAt: time.Now(), HoldSeconds: 600, HomeRegion: "local",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Rebuild(context.Background(), r.db, r.inv); err != nil {
		t.Fatal(err)
	}
	r.fake = psp.NewFake(fakeCfg)
	cc := psp.DefaultClientConfig()
	cc.AttemptTimeout = 500 * time.Millisecond
	cc.MaxAttempts = 2
	cc.BackoffBase, cc.BackoffCap = 5*time.Millisecond, 20*time.Millisecond
	// The fault rates below are injected on purpose; keep the breaker from masking them.
	cc.Breaker.ReadyToTrip = func(gobreaker.Counts) bool { return false }
	r.client = psp.NewClient(r.fake, cc)
	r.checkout = checkout.NewService(r.db, r.inv, 9900)
	r.resolver = payment.NewResolver(r.db, r.inv, r.client, r.log)
	rc := reconcile.DefaultConfig()
	rc.PendingAfter, rc.GiveUpAfter = 0, time.Hour
	r.rec = reconcile.New(r.db, r.inv, r.client, r.resolver, rc, r.log)
	return r
}

func (r *rig) worker() *payment.Worker {
	wc := payment.DefaultWorkerConfig()
	wc.DropID = r.inv.DropID()
	wc.Lease = 2 * time.Second
	wc.RetryBase, wc.RetryCap = 5*time.Millisecond, 50*time.Millisecond
	wc.MaxAttempts = 30
	return payment.NewWorker(r.db, r.client, r.resolver, wc, r.log)
}

func (r *rig) pendingCommands(t *testing.T) int {
	var n int
	if err := r.db.QueryRow(context.Background(), `SELECT count(*) FROM outbox
		WHERE event_type = 'PaymentRequested' AND processed_at IS NULL`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (r *rig) count(t *testing.T, q string, args ...any) int {
	var n int
	if err := r.db.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestEndToEnd_UnreliablePSP_ConvergesWithNoLostOrDoubleCharges drives 3,000 buyers at 2,000 units
// through reserve → checkout → payment with a PSP that times out after charging 20% of the time,
// rejects 10% of calls, declines 5% of cards, plus duplicate checkout submits, competing workers and
// simulated worker crashes. At the end:
//
//	oversold = 0, users with two units = 0, duplicate charges = 0, charged without order = 0,
//	PAID orders = successful charges = allocations, and declined units are back in Redis.
func TestEndToEnd_UnreliablePSP_ConvergesWithNoLostOrDoubleCharges(t *testing.T) {
	const units, buyers = 2000, 3000
	ctx := context.Background()
	r := newRig(t, units, 10, psp.FakeConfig{
		Latency: 2 * time.Millisecond, TimeoutAfterChargeRate: 0.20, UnavailableRate: 0.10, DeclineRate: 0.05,
	})

	// 1. Everyone reserves at once.
	var mu sync.Mutex
	var held []inventory.Reservation
	var wg sync.WaitGroup
	sem := make(chan struct{}, 200)
	for i := 0; i < buyers; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			res, err := r.inv.Reserve(ctx, "buyer-"+itoa(i))
			if err == nil {
				mu.Lock()
				held = append(held, res)
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if len(held) != units {
		t.Fatalf("reserved %d, want %d", len(held), units)
	}

	// 2. Every holder submits payment; one in ten double-submits concurrently (impatient user).
	var submitted atomic.Int64
	for i, h := range held {
		n := 1
		if i%10 == 0 {
			n = 2
		}
		for k := 0; k < n; k++ {
			wg.Add(1)
			sem <- struct{}{}
			go func(h inventory.Reservation) {
				defer wg.Done()
				defer func() { <-sem }()
				if _, err := r.checkout.Submit(ctx, h.ID, h.UserID, "tok_ok"); err == nil {
					submitted.Add(1)
				} else {
					t.Errorf("submit %s: %v", h.ID, err)
				}
			}(h)
		}
	}
	wg.Wait()
	if got := r.count(t, `SELECT count(*) FROM payments`); got != units {
		t.Fatalf("payments rows = %d, want %d (duplicate submits must not create payments)", got, units)
	}

	// 3. Simulate workers that crashed after charging but before recording the result: the PSP has
	//    the charge, our database does not.
	for _, h := range held[:100] {
		_, _ = r.fake.Charge(ctx, h.ID, 9900, "tok_ok")
	}

	// 4. Two competing workers drain the commands.
	wctx, stop := context.WithCancel(ctx)
	var wwg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wwg.Add(1)
		go func() { defer wwg.Done(); _ = r.worker().Run(wctx) }()
	}
	deadline := time.Now().Add(90 * time.Second)
	for r.pendingCommands(t) > 0 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	stop()
	wwg.Wait()
	if n := r.pendingCommands(t); n > 0 {
		t.Fatalf("%d payment commands still pending after 90 s", n)
	}

	// 5. The reconciler resolves anything workers gave up on and resends refunds.
	if _, err := r.rec.Once(ctx); err != nil {
		t.Fatal(err)
	}

	// 6. Invariants.
	inv, err := r.rec.Check(ctx, units, r.fake.Ledger())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("invariants: %+v", inv)
	if inv != (reconcile.Invariants{}) {
		t.Fatalf("invariants violated: %+v", inv)
	}

	paid := r.count(t, `SELECT count(*) FROM orders WHERE state = 'PAID'`)
	failed := r.count(t, `SELECT count(*) FROM orders WHERE state = 'FAILED'`)
	pending := r.count(t, `SELECT count(*) FROM orders WHERE state = 'PAYMENT_PENDING'`)
	allocations := r.count(t, `SELECT count(*) FROM allocations`)
	successfulCharges := 0
	for _, c := range r.fake.Ledger() {
		if c.Status == psp.StatusSucceeded && !c.Refunded {
			successfulCharges++
		}
	}
	left, _ := r.inv.Available(ctx)
	t.Logf("paid=%d failed=%d pending=%d allocations=%d successful_charges=%d psp_calls=%d redis_available=%d",
		paid, failed, pending, allocations, successfulCharges, r.fake.Calls(), left)

	if pending != 0 {
		t.Fatalf("%d orders still PAYMENT_PENDING", pending)
	}
	if paid+failed != units {
		t.Fatalf("paid %d + failed %d != %d", paid, failed, units)
	}
	if paid != successfulCharges {
		t.Fatalf("paid orders %d != successful charges %d", paid, successfulCharges)
	}
	if allocations != paid {
		t.Fatalf("allocations %d != paid orders %d (declined orders must free their unit)", allocations, paid)
	}
	if int(left) != failed {
		t.Fatalf("redis available %d != declined orders %d (declined units must return to stock)", left, failed)
	}
}

// Failure path E: a Redis failover rolls back an LPOP, so the same unit is handed to two users.
// Postgres rejects the second allocation BEFORE any charge, and the unit is not pushed back.
func TestBackstop_RedisHandsOutSameUnitTwice_SecondUserNeverCharged(t *testing.T) {
	ctx := context.Background()
	r := newRig(t, 1, 1, psp.FakeConfig{})

	a, err := r.inv.Reserve(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	// Simulate the lost write: the unit reappears in the bucket.
	key := "inv:{" + r.inv.DropID() + ":00}:units"
	if err := r.rdb.RPush(ctx, key, a.UnitID).Err(); err != nil {
		t.Fatal(err)
	}
	b, err := r.inv.Reserve(ctx, "bob")
	if err != nil || b.UnitID != a.UnitID {
		t.Fatalf("setup: bob got (%v, %v), want the same unit %s", b.UnitID, err, a.UnitID)
	}

	if _, err := r.checkout.Submit(ctx, a.ID, "alice", "tok_ok"); err != nil {
		t.Fatalf("alice: %v", err)
	}
	if _, err := r.checkout.Submit(ctx, b.ID, "bob", "tok_ok"); !errors.Is(err, checkout.ErrUnitTaken) {
		t.Fatalf("bob: got %v, want ErrUnitTaken", err)
	}
	if n := r.count(t, `SELECT count(*) FROM payments`); n != 1 {
		t.Fatalf("payments = %d, want 1 (bob must never reach the PSP)", n)
	}
	if n, _ := r.inv.Available(ctx); n != 0 {
		t.Fatalf("available = %d, want 0 (the duplicated unit must not be pushed back)", n)
	}
}

// Redis loses everything in the middle of a drop. Rebuilding from Postgres gives back exactly the free
// units, stops a buyer from buying twice, and lets a payment that was in progress finish normally.
func TestRebuildFromPostgres_AfterRedisLosesEverything(t *testing.T) {
	ctx := context.Background()
	r := newRig(t, 10, 2, psp.FakeConfig{})

	// alice has paid; bob's payment is still in progress; carol only holds a unit.
	a, err := r.inv.Reserve(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.checkout.Submit(ctx, a.ID, "alice", "tok_ok"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.worker().ProcessBatch(ctx); err != nil {
		t.Fatal(err)
	}
	b, err := r.inv.Reserve(ctx, "bob")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.checkout.Submit(ctx, b.ID, "bob", "tok_ok"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.inv.Reserve(ctx, "carol"); err != nil {
		t.Fatal(err)
	}

	if err := r.inv.Reset(ctx); err != nil { // Redis loses all its data
		t.Fatal(err)
	}
	free, err := catalog.Rebuild(ctx, r.db, r.inv)
	if err != nil {
		t.Fatal(err)
	}
	if free != 8 {
		t.Fatalf("free = %d, want 8 (10 units minus alice's and bob's)", free)
	}
	if n, _ := r.inv.Available(ctx); n != 8 {
		t.Fatalf("available = %d, want 8", n)
	}

	// alice already bought: she gets her old reservation back, not a second unit.
	var held *inventory.AlreadyHeldError
	if _, err := r.inv.Reserve(ctx, "alice"); !errors.As(err, &held) || held.ReservationID != a.ID {
		t.Fatalf("alice: got %v, want AlreadyHeld(%s)", err, a.ID)
	}

	// bob's payment finishes normally after the rebuild, and his unit stays sold.
	if _, err := r.worker().ProcessBatch(ctx); err != nil {
		t.Fatal(err)
	}
	if n := r.count(t, `SELECT count(*) FROM orders WHERE state = 'PAID'`); n != 2 {
		t.Fatalf("paid orders = %d, want 2", n)
	}
	if n, _ := r.inv.Available(ctx); n != 8 {
		t.Fatalf("available after bob's payment = %d, want 8", n)
	}

	// carol's unpaid hold lived only in Redis and was lost: she can hold again.
	if _, err := r.inv.Reserve(ctx, "carol"); err != nil {
		t.Fatalf("carol: %v", err)
	}
}

// Failure path D: the reconciler closes a payment the PSP has never seen, then a late charge for it
// arrives. The late charge is refunded automatically.
func TestLateChargeAfterPaymentClosed_IsRefunded(t *testing.T) {
	ctx := context.Background()
	r := newRig(t, 1, 1, psp.FakeConfig{})
	rc := reconcile.DefaultConfig()
	rc.PendingAfter, rc.GiveUpAfter = 0, 0
	rec := reconcile.New(r.db, r.inv, r.client, r.resolver, rc, r.log)

	h, _ := r.inv.Reserve(ctx, "alice")
	if _, err := r.checkout.Submit(ctx, h.ID, "alice", "tok_ok"); err != nil {
		t.Fatal(err)
	}
	if _, closed, err := rec.ResolvePending(ctx); err != nil || closed != 1 {
		t.Fatalf("closed %d (err %v), want 1", closed, err)
	}
	if n, _ := r.inv.Available(ctx); n != 1 {
		t.Fatalf("unit not released after close: available = %d", n)
	}

	ch, _ := r.fake.Charge(ctx, h.ID, 9900, "tok_ok") // the late charge
	out, err := r.resolver.Apply(ctx, h.ID, ch, nil)
	if err != nil || out != payment.OutcomeRefunded {
		t.Fatalf("apply late charge: (%v, %v), want REFUNDED", out, err)
	}
	inv, _ := rec.Check(ctx, 1, r.fake.Ledger())
	if inv.ChargedWithoutOrder != 0 {
		t.Fatalf("charged without order = %d, want 0", inv.ChargedWithoutOrder)
	}
	if n := r.count(t, `SELECT count(*) FROM refunds WHERE sent_at IS NULL`); n != 0 {
		t.Fatalf("unsent refunds = %d, want 0 (an accepted refund must be marked sent)", n)
	}

	// The immediate refund call is lost: the reconciler sends it once, then stops resending it.
	if _, err := r.db.Exec(ctx, `UPDATE refunds SET sent_at = NULL`); err != nil {
		t.Fatal(err)
	}
	if n, err := rec.SendRefunds(ctx); err != nil || n != 1 {
		t.Fatalf("first pass sent %d (err %v), want 1", n, err)
	}
	if n, err := rec.SendRefunds(ctx); err != nil || n != 0 {
		t.Fatalf("second pass sent %d (err %v), want 0", n, err)
	}
}

// A hold that expired cannot start a payment, and nothing is written for it.
func TestCheckout_ExpiredHoldIsRejectedBeforeAnyWrite(t *testing.T) {
	ctx := context.Background()
	r := newRig(t, 1, 1, psp.FakeConfig{})
	short := inventory.NewStore(r.rdb, inventory.Options{DropID: r.inv.DropID(), Buckets: 1, HoldTTL: time.Millisecond})
	h, err := short.Reserve(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	co := checkout.NewService(r.db, short, 9900)
	if _, err := co.Submit(ctx, h.ID, "alice", "tok_ok"); !errors.Is(err, checkout.ErrExpired) {
		t.Fatalf("got %v, want ErrExpired", err)
	}
	if n := r.count(t, `SELECT count(*) FROM orders`); n != 0 {
		t.Fatalf("orders = %d, want 0", n)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
