package inventory_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"reservation-service/internal/inventory"
	"reservation-service/internal/testutil"
)

// TestReserve_200KConcurrentAttempts is the core proof of the inventory gate.
//
// 100,000 users each try twice (200,000 attempts) to reserve one of 50,000 units, from 512
// goroutines at once against a real Redis. The test asserts:
//   - exactly 50,000 reservations succeed (no oversell, no stranded units)
//   - every reserved unit ID is unique (no unit handed out twice)
//   - no user holds more than one unit, and a user's second attempt returns their first reservation
//   - Redis reports 0 units left
func TestReserve_200KConcurrentAttempts(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 200K-attempt test in -short mode")
	}
	const (
		units       = 50_000
		buckets     = 50
		users       = 100_000
		attemptsPer = 2
		workers     = 512
	)
	ctx := context.Background()
	rdb := testutil.Redis(t, workers+16)
	store := inventory.NewStore(rdb, inventory.Options{
		DropID: testutil.DropID("conc"), Buckets: buckets, HoldTTL: 10 * time.Minute,
	})
	t.Cleanup(func() { _ = store.Reset(context.Background()) })
	if err := store.Seed(ctx, units); err != nil {
		t.Fatal(err)
	}

	// Every user appears twice, in random order, so duplicate attempts race each other.
	attempts := make([]int, 0, users*attemptsPer)
	for u := 0; u < users; u++ {
		for i := 0; i < attemptsPer; i++ {
			attempts = append(attempts, u)
		}
	}
	rand.Shuffle(len(attempts), func(i, j int) { attempts[i], attempts[j] = attempts[j], attempts[i] })

	type win struct {
		user int
		res  inventory.Reservation
	}
	var (
		ok, alreadyHeld, soldOut, busy, other atomic.Int64
		mu                                    sync.Mutex
		wins                                  = make([]win, 0, units)
		replies                               = make(map[int][]string) // user -> reservation IDs returned on ALREADY_HELD
		firstErr                              atomic.Value
	)

	jobs := make(chan int, 4096)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for u := range jobs {
				user := userID(u)
				res, err := store.Reserve(ctx, user)
				var held *inventory.AlreadyHeldError
				switch {
				case err == nil:
					ok.Add(1)
					mu.Lock()
					wins = append(wins, win{u, res})
					mu.Unlock()
				case errors.As(err, &held):
					alreadyHeld.Add(1)
					mu.Lock()
					replies[u] = append(replies[u], held.ReservationID)
					mu.Unlock()
				case errors.Is(err, inventory.ErrSoldOut):
					soldOut.Add(1)
				case errors.Is(err, inventory.ErrBusy):
					busy.Add(1) // same user's other attempt was mid-flight
				default:
					other.Add(1)
					firstErr.CompareAndSwap(nil, err)
				}
			}
		}()
	}
	for _, u := range attempts {
		jobs <- u
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(start)

	t.Logf("%d attempts in %s (%.0f attempts/s): ok=%d already_held=%d sold_out=%d busy=%d other=%d",
		len(attempts), elapsed.Round(time.Millisecond), float64(len(attempts))/elapsed.Seconds(),
		ok.Load(), alreadyHeld.Load(), soldOut.Load(), busy.Load(), other.Load())

	if e := firstErr.Load(); e != nil {
		t.Fatalf("unexpected error: %v", e)
	}
	if got := ok.Load(); got != units {
		t.Fatalf("successful reservations = %d, want exactly %d", got, units)
	}

	unitOwner := make(map[string]int, units)
	userRes := make(map[int]string, units)
	for _, w := range wins {
		if prev, dup := unitOwner[w.res.UnitID]; dup {
			t.Fatalf("unit %s reserved by user %d and user %d", w.res.UnitID, prev, w.user)
		}
		unitOwner[w.res.UnitID] = w.user
		if prev, dup := userRes[w.user]; dup {
			t.Fatalf("user %d holds two reservations: %s and %s", w.user, prev, w.res.ID)
		}
		userRes[w.user] = w.res.ID
	}
	for u, ids := range replies {
		for _, id := range ids {
			if id != userRes[u] {
				t.Fatalf("user %d retry returned %s, but their reservation is %s", u, id, userRes[u])
			}
		}
	}
	left, err := store.Available(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Fatalf("units left in Redis = %d, want 0", left)
	}
}

func userID(u int) string { return "user-" + itoa(u) }

func itoa(n int) string {
	const digits = "0123456789"
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = digits[n%10]
		n /= 10
	}
	return string(b[i:])
}
