package inventory_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"reservation-service/internal/inventory"
	"reservation-service/internal/testutil"
)

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) Advance(d time.Duration) { c.mu.Lock(); c.now = c.now.Add(d); c.mu.Unlock() }

func newStore(t *testing.T, units, buckets int, ttl time.Duration) (*inventory.Store, *clock) {
	t.Helper()
	rdb := testutil.Redis(t, 32)
	clk := &clock{now: time.Now()}
	s := inventory.NewStore(rdb, inventory.Options{
		DropID: testutil.DropID("unit"), Buckets: buckets, HoldTTL: ttl, Now: clk.Now,
	})
	t.Cleanup(func() { _ = s.Reset(context.Background()) })
	if err := s.Seed(context.Background(), units); err != nil {
		t.Fatal(err)
	}
	return s, clk
}

func TestReserve_SameUserGetsSameReservation(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t, 10, 2, time.Minute)

	first, err := s.Reserve(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Reserve(ctx, "alice")
	var held *inventory.AlreadyHeldError
	if !errors.As(err, &held) || held.ReservationID != first.ID {
		t.Fatalf("second reserve: got %v, want AlreadyHeld(%s)", err, first.ID)
	}
	if n, _ := s.Available(ctx); n != 9 {
		t.Fatalf("available = %d, want 9", n)
	}
}

func TestReserve_FallsBackWhenHomeBucketEmpty(t *testing.T) {
	ctx := context.Background()
	// 3 units over 3 buckets: most users' home bucket empties quickly and they must find units elsewhere.
	s, _ := newStore(t, 3, 3, time.Minute)
	got := 0
	for i := 0; i < 50 && got < 3; i++ {
		if _, err := s.Reserve(ctx, userID(i)); err == nil {
			got++
		}
	}
	if got != 3 {
		t.Fatalf("reserved %d, want all 3 units found across buckets", got)
	}
	if _, err := s.Reserve(ctx, "late-user"); !errors.Is(err, inventory.ErrSoldOut) {
		t.Fatalf("after sell-out: got %v, want ErrSoldOut", err)
	}
}

func TestExpireDue_ReturnsUnitsForResale(t *testing.T) {
	ctx := context.Background()
	s, clk := newStore(t, 10, 2, time.Minute)

	first := map[string]bool{}
	for i := 0; i < 10; i++ {
		r, err := s.Reserve(ctx, userID(i))
		if err != nil {
			t.Fatal(err)
		}
		first[r.UnitID] = true
	}
	if _, err := s.Reserve(ctx, "user-x"); !errors.Is(err, inventory.ErrSoldOut) {
		t.Fatalf("got %v, want ErrSoldOut", err)
	}

	clk.Advance(61 * time.Second)
	n, err := s.ExpireDue(ctx, 100)
	if err != nil || n != 10 {
		t.Fatalf("expired %d (err %v), want 10", n, err)
	}

	// The same 10 units are sold to 10 new users, never more.
	second := map[string]bool{}
	for i := 100; i < 120; i++ {
		r, err := s.Reserve(ctx, userID(i))
		if err == nil {
			second[r.UnitID] = true
		}
	}
	if len(second) != 10 {
		t.Fatalf("resold %d units, want 10", len(second))
	}
	for u := range second {
		if !first[u] {
			t.Fatalf("unit %s did not exist in the original stock", u)
		}
	}
	// A user whose hold expired may reserve again.
	if _, err := s.Reserve(ctx, userID(0)); !errors.Is(err, inventory.ErrSoldOut) {
		t.Fatalf("got %v, want ErrSoldOut (all units re-held)", err)
	}
}

func TestFreeze_HoldCannotExpireDuringPayment(t *testing.T) {
	ctx := context.Background()
	s, clk := newStore(t, 1, 1, time.Minute)

	r, err := s.Reserve(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Freeze(ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Hour)
	if n, _ := s.ExpireDue(ctx, 100); n != 0 {
		t.Fatalf("expired %d frozen holds, want 0", n)
	}
	if n, _ := s.Available(ctx); n != 0 {
		t.Fatalf("available = %d, want 0 while payment is in flight", n)
	}
	// Freezing again is idempotent.
	if _, err := s.Freeze(ctx, r.ID); err != nil {
		t.Fatalf("second freeze: %v", err)
	}
}

func TestFreeze_RejectsExpiredHold(t *testing.T) {
	ctx := context.Background()
	s, clk := newStore(t, 1, 1, time.Minute)
	r, err := s.Reserve(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Minute)
	if _, err := s.Freeze(ctx, r.ID); !errors.Is(err, inventory.ErrExpired) {
		t.Fatalf("got %v, want ErrExpired", err)
	}
}

func TestRelease_ReturnsUnitAndAllowsNewReservation(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t, 1, 1, time.Minute)
	r, err := s.Reserve(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Reserve(ctx, "bob"); err != nil {
		t.Fatalf("bob after release: %v", err)
	}
	if _, err := s.Reserve(ctx, "alice"); !errors.Is(err, inventory.ErrSoldOut) {
		t.Fatalf("alice after release: got %v, want ErrSoldOut", err)
	}
}

func TestComplete_BlocksSecondPurchase(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t, 5, 1, time.Minute)
	r, err := s.Reserve(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Complete(ctx, r.ID, "alice"); err != nil {
		t.Fatal(err)
	}
	var held *inventory.AlreadyHeldError
	if _, err := s.Reserve(ctx, "alice"); !errors.As(err, &held) || held.ReservationID != r.ID {
		t.Fatalf("got %v, want AlreadyHeld(%s)", err, r.ID)
	}
}

// The reconciler may unfreeze an "orphan" just as its checkout commits and the hold is completed.
// Unfreeze must then do nothing, and never create a partial hold the reaper would choke on.
func TestUnfreeze_MissingOrCompletedHoldIsNoOp(t *testing.T) {
	ctx := context.Background()
	s, clk := newStore(t, 2, 1, time.Minute)

	if err := s.Unfreeze(ctx, "00-does-not-exist", time.Minute); err != nil {
		t.Fatalf("unfreeze missing hold: %v", err)
	}
	r, err := s.Reserve(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Freeze(ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Complete(ctx, r.ID, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := s.Unfreeze(ctx, r.ID, time.Minute); err != nil {
		t.Fatalf("unfreeze completed hold: %v", err)
	}
	if _, _, err := s.Get(ctx, r.ID); !errors.Is(err, inventory.ErrNotFound) {
		t.Fatalf("hold reappeared after unfreeze: %v", err)
	}
	clk.Advance(2 * time.Minute)
	if _, err := s.ExpireDue(ctx, 100); err != nil {
		t.Fatalf("reaper after unfreeze of completed hold: %v", err)
	}
	if n, _ := s.Available(ctx); n != 1 {
		t.Fatalf("available = %d, want 1 (the sold unit must not come back)", n)
	}
}

func TestUnfreeze_PutsFrozenHoldBackOnTimer(t *testing.T) {
	ctx := context.Background()
	s, clk := newStore(t, 1, 1, time.Minute)
	r, _ := s.Reserve(ctx, "alice")
	if _, err := s.Freeze(ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Unfreeze(ctx, r.ID, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	clk.Advance(31 * time.Second)
	if n, _ := s.ExpireDue(ctx, 100); n != 1 {
		t.Fatalf("expired %d, want 1", n)
	}
	if n, _ := s.Available(ctx); n != 1 {
		t.Fatalf("available = %d, want 1", n)
	}
}

func TestComplete_IsIdempotent(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t, 1, 1, time.Minute)
	r, _ := s.Reserve(ctx, "alice")
	for i := 0; i < 3; i++ {
		if err := s.Complete(ctx, r.ID, "alice"); err != nil {
			t.Fatalf("complete #%d: %v", i+1, err)
		}
	}
}

func BenchmarkReserve(b *testing.B) {
	ctx := context.Background()
	rdb := testutil.Redis(b, 256)
	s := inventory.NewStore(rdb, inventory.Options{DropID: testutil.DropID("bench"), Buckets: 50, HoldTTL: time.Hour})
	b.Cleanup(func() { _ = s.Reset(context.Background()) })
	if err := s.Seed(ctx, b.N+1000); err != nil {
		b.Fatal(err)
	}
	var n int64
	var mu sync.Mutex
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			n++
			id := n
			mu.Unlock()
			if _, err := s.Reserve(ctx, "bench-"+itoa(int(id))); err != nil {
				b.Error(err)
			}
		}
	})
}
