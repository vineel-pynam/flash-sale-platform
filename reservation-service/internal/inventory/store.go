// Package inventory is the fast inventory gate: unit-ID buckets in Redis, reserved with atomic Lua
// scripts. Postgres remains the source of truth (see internal/checkout and docs/adr/0002).
package inventory

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

//go:embed scripts/*.lua
var scriptFS embed.FS

func mustScript(name string) *redis.Script {
	b, err := scriptFS.ReadFile("scripts/" + name)
	if err != nil {
		panic(err)
	}
	return redis.NewScript(string(b))
}

var (
	reserveScript  = mustScript("reserve.lua")
	popHoldScript  = mustScript("pop_hold.lua")
	markerScript   = mustScript("marker.lua")
	freezeScript   = mustScript("freeze.lua")
	releaseScript  = mustScript("release.lua")
	completeScript = mustScript("complete.lua")
	unfreezeScript = mustScript("unfreeze.lua")
)

var (
	ErrSoldOut  = errors.New("inventory: sold out")
	ErrBusy     = errors.New("inventory: another request for this user is in progress")
	ErrNotFound = errors.New("inventory: reservation not found")
	ErrExpired  = errors.New("inventory: reservation expired")
)

// AlreadyHeldError is returned when the user already holds (or bought) a unit in this drop.
// It carries the existing reservation so a retried request gets the same answer.
type AlreadyHeldError struct{ ReservationID string }

func (e *AlreadyHeldError) Error() string {
	return "inventory: user already holds reservation " + e.ReservationID
}

// Reservation is a hold on one unit.
type Reservation struct {
	ID        string
	DropID    string
	UserID    string
	UnitID    string
	Bucket    int
	ExpiresAt time.Time
}

// Options configures a Store.
type Options struct {
	DropID  string
	Buckets int
	HoldTTL time.Duration
	// HomeProbes is how many neighbouring buckets to try before falling back to the
	// non-empty set and, finally, a full scan.
	HomeProbes int
	Now        func() time.Time
}

// Store reserves units for one drop.
type Store struct {
	rdb  redis.UniversalClient
	opts Options
}

const (
	pendingClaimTTL = 5 * time.Second
	markerGrace     = time.Minute
	soldMarkerPref  = "sold:"
)

func NewStore(rdb redis.UniversalClient, opts Options) *Store {
	if opts.HomeProbes <= 0 {
		opts.HomeProbes = 2
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Store{rdb: rdb, opts: opts}
}

func (s *Store) Buckets() int   { return s.opts.Buckets }
func (s *Store) DropID() string { return s.opts.DropID }

// ---- key layout (docs/03-entities-apis-schema.md, section 3) ----

func (s *Store) tag(b int) string               { return fmt.Sprintf("{%s:%02d}", s.opts.DropID, b) }
func (s *Store) unitsKey(b int) string          { return "inv:" + s.tag(b) + ":units" }
func (s *Store) userKey(b int, u string) string { return "inv:" + s.tag(b) + ":user:" + u }
func (s *Store) holdKey(b int, r string) string { return "inv:" + s.tag(b) + ":hold:" + r }
func (s *Store) expiryKey(b int) string         { return "inv:" + s.tag(b) + ":expiry" }
func (s *Store) nonEmptyKey() string            { return "inv:{" + s.opts.DropID + "}:nonempty" }
func (s *Store) soldOutKey() string             { return "inv:{" + s.opts.DropID + "}:soldout" }

// HomeBucket maps a user to the bucket that holds their one-per-account marker.
func (s *Store) HomeBucket(userID string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(userID))
	return int(h.Sum32() % uint32(s.opts.Buckets))
}

// newReservationID embeds the bucket so any service can route to the right shard without a lookup.
func newReservationID(bucket int) string {
	return fmt.Sprintf("%02d-%s", bucket, uuid.NewString())
}

// BucketOf extracts the bucket from a reservation ID.
func BucketOf(reservationID string) (int, error) {
	prefix, _, ok := strings.Cut(reservationID, "-")
	if !ok {
		return 0, ErrNotFound
	}
	b, err := strconv.Atoi(prefix)
	if err != nil || b < 0 {
		return 0, ErrNotFound
	}
	return b, nil
}

func (s *Store) bucketOf(reservationID string) (int, error) {
	b, err := BucketOf(reservationID)
	if err != nil || b >= s.opts.Buckets {
		return 0, ErrNotFound
	}
	return b, nil
}

// ---- seeding ----

// Seed loads units into buckets, replacing any existing state for the drop. Unit IDs are
// U000001..UNNNNNN, split into contiguous blocks per bucket. Redis-only tests use it; the server
// loads Redis from Postgres instead (internal/catalog).
func (s *Store) Seed(ctx context.Context, units int) error {
	per := units / s.opts.Buckets
	extra := units % s.opts.Buckets
	next := 1
	byBucket := make(map[int][]string, s.opts.Buckets)
	for b := 0; b < s.opts.Buckets; b++ {
		n := per
		if b < extra {
			n++
		}
		ids := make([]string, 0, n)
		for i := 0; i < n; i++ {
			ids = append(ids, fmt.Sprintf("U%06d", next))
			next++
		}
		byBucket[b] = ids
	}
	return s.Load(ctx, byBucket)
}

// Load replaces the drop's Redis state with the given free units per bucket.
func (s *Store) Load(ctx context.Context, byBucket map[int][]string) error {
	if err := s.Reset(ctx); err != nil {
		return err
	}
	for b, units := range byBucket {
		if b < 0 || b >= s.opts.Buckets {
			return fmt.Errorf("inventory: bucket %d out of range (drop has %d buckets)", b, s.opts.Buckets)
		}
		if len(units) == 0 {
			continue
		}
		for start := 0; start < len(units); start += 1000 {
			end := min(start+1000, len(units))
			ids := make([]any, 0, end-start)
			for _, u := range units[start:end] {
				ids = append(ids, u)
			}
			if err := s.rdb.RPush(ctx, s.unitsKey(b), ids...).Err(); err != nil {
				return err
			}
		}
		if err := s.rdb.SAdd(ctx, s.nonEmptyKey(), b).Err(); err != nil {
			return err
		}
	}
	return nil
}

// MarkSold records that the user bought a unit, so they cannot hold another one. Used by the
// rebuild for PAID orders.
func (s *Store) MarkSold(ctx context.Context, userID, reservationID string) error {
	return s.rdb.Set(ctx, s.userKey(s.HomeBucket(userID), userID), soldMarkerPref+reservationID, 0).Err()
}

// RestoreFrozen re-creates a hold whose payment is still in progress, so the payment result can
// complete or release it as usual. Used by the rebuild for PAYMENT_PENDING orders.
func (s *Store) RestoreFrozen(ctx context.Context, reservationID, userID, unitID string) error {
	b, err := s.bucketOf(reservationID)
	if err != nil {
		return err
	}
	now := strconv.FormatInt(s.opts.Now().UnixMilli(), 10)
	if err := s.rdb.HSet(ctx, s.holdKey(b, reservationID),
		"user", userID, "unit", unitID, "state", "FROZEN", "exp", now, "frozen_at", now).Err(); err != nil {
		return err
	}
	return s.rdb.Set(ctx, s.userKey(s.HomeBucket(userID), userID), reservationID, 0).Err()
}

// Reset deletes every key for the drop. Used by Seed and tests.
func (s *Store) Reset(ctx context.Context) error {
	patterns := []string{"inv:{" + s.opts.DropID + ":*", "inv:{" + s.opts.DropID + "}:*"}
	for _, p := range patterns {
		iter := s.rdb.Scan(ctx, 0, p, 1000).Iterator()
		batch := make([]string, 0, 1000)
		for iter.Next(ctx) {
			batch = append(batch, iter.Val())
			if len(batch) == 1000 {
				if err := s.rdb.Unlink(ctx, batch...).Err(); err != nil {
					return err
				}
				batch = batch[:0]
			}
		}
		if err := iter.Err(); err != nil {
			return err
		}
		if len(batch) > 0 {
			if err := s.rdb.Unlink(ctx, batch...).Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---- reserve ----

// Reserve holds one unit for the user. It is safe to call repeatedly: a user who already holds a
// unit gets *AlreadyHeldError with the same reservation ID.
func (s *Store) Reserve(ctx context.Context, userID string) (Reservation, error) {
	if soldOut, _ := s.rdb.Exists(ctx, s.soldOutKey()).Result(); soldOut == 1 {
		return Reservation{}, ErrSoldOut
	}

	home := s.HomeBucket(userID)
	rid := newReservationID(home)
	exp := s.opts.Now().Add(s.opts.HoldTTL)
	markerTTL := s.opts.HoldTTL + markerGrace

	res, err := reserveScript.Run(ctx, s.rdb,
		[]string{s.unitsKey(home), s.userKey(home, userID), s.holdKey(home, rid), s.expiryKey(home)},
		userID, rid, exp.UnixMilli(), markerTTL.Milliseconds(), pendingClaimTTL.Milliseconds(),
	).StringSlice()
	if err != nil {
		return Reservation{}, fmt.Errorf("reserve script: %w", err)
	}

	switch res[0] {
	case "OK":
		return Reservation{ID: rid, DropID: s.opts.DropID, UserID: userID, UnitID: res[1], Bucket: home, ExpiresAt: exp}, nil
	case "ALREADY_HELD":
		return Reservation{}, &AlreadyHeldError{ReservationID: strings.TrimPrefix(res[1], soldMarkerPref)}
	case "BUSY":
		return Reservation{}, ErrBusy
	case "EMPTY_CLAIMED":
		return s.reserveElsewhere(ctx, userID, home, "pending:"+rid, exp, markerTTL)
	default:
		return Reservation{}, fmt.Errorf("reserve script: unexpected status %q", res[0])
	}
}

// reserveElsewhere runs when the home bucket is empty. The user's marker has been claimed with a
// short-lived "pending" value, so concurrent requests for the same user get ErrBusy. This path is
// not one atomic step, but its only failure mode is a user briefly unable to reserve; it can never
// hand out an extra unit, and Postgres UNIQUE(event_id, user_id) backs up the one-per-account rule.
func (s *Store) reserveElsewhere(ctx context.Context, userID string, home int, pending string, exp time.Time, markerTTL time.Duration) (Reservation, error) {
	marker := s.userKey(home, userID)

	for _, b := range s.candidateBuckets(ctx, home) {
		rid := newReservationID(b)
		unit, err := popHoldScript.Run(ctx, s.rdb,
			[]string{s.unitsKey(b), s.holdKey(b, rid), s.expiryKey(b)},
			userID, rid, exp.UnixMilli(),
		).Text()
		if errors.Is(err, redis.Nil) {
			s.markMaybeEmpty(ctx, b)
			continue
		}
		if err != nil {
			_ = s.casMarker(ctx, marker, pending, "delete", "", 0)
			return Reservation{}, fmt.Errorf("pop_hold script: %w", err)
		}

		// Point the marker at the new reservation, but only if our claim is still the one there.
		if err := s.casMarker(ctx, marker, pending, "set", rid, markerTTL); err != nil {
			// Our claim lapsed (very slow request). Give the unit back rather than risk two holds.
			_, _ = s.releaseHold(ctx, b, rid, "release", 0)
			return Reservation{}, ErrBusy
		}
		return Reservation{ID: rid, DropID: s.opts.DropID, UserID: userID, UnitID: unit, Bucket: b, ExpiresAt: exp}, nil
	}

	_ = s.casMarker(ctx, marker, pending, "delete", "", 0)
	// Nothing found anywhere. Mark sold out for a short time so later requests return fast.
	// Any release clears the flag immediately.
	s.rdb.Set(ctx, s.soldOutKey(), 1, 2*time.Second)
	return Reservation{}, ErrSoldOut
}

// candidateBuckets returns neighbours of home first, then buckets believed non-empty, then every
// bucket, so a remaining unit is always found while it exists.
func (s *Store) candidateBuckets(ctx context.Context, home int) []int {
	n := s.opts.Buckets
	seen := make(map[int]bool, n)
	seen[home] = true
	out := make([]int, 0, n)
	add := func(b int) {
		if !seen[b] {
			seen[b] = true
			out = append(out, b)
		}
	}
	for i := 1; i <= s.opts.HomeProbes && i < n; i++ {
		add((home + i) % n)
	}
	if members, err := s.rdb.SRandMemberN(ctx, s.nonEmptyKey(), 4).Result(); err == nil {
		for _, m := range members {
			if b, err := strconv.Atoi(m); err == nil && b < n {
				add(b)
			}
		}
	}
	start := rand.IntN(n)
	for i := 0; i < n; i++ {
		add((start + i) % n)
	}
	return out
}

func (s *Store) markMaybeEmpty(ctx context.Context, b int) {
	s.rdb.SRem(ctx, s.nonEmptyKey(), b)
	// A release may have pushed a unit between our LPOP and SREM; re-add if so.
	if n, err := s.rdb.LLen(ctx, s.unitsKey(b)).Result(); err == nil && n > 0 {
		s.rdb.SAdd(ctx, s.nonEmptyKey(), b)
	}
}

var errMarkerChanged = errors.New("inventory: marker changed")

func (s *Store) casMarker(ctx context.Context, key, expect, action, value string, ttl time.Duration) error {
	n, err := markerScript.Run(ctx, s.rdb, []string{key}, expect, action, value, ttl.Milliseconds()).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return errMarkerChanged
	}
	return nil
}

// ---- lifecycle after reserve ----

// Get returns a hold by reservation ID.
func (s *Store) Get(ctx context.Context, reservationID string) (Reservation, string, error) {
	b, err := s.bucketOf(reservationID)
	if err != nil {
		return Reservation{}, "", err
	}
	h, err := s.rdb.HGetAll(ctx, s.holdKey(b, reservationID)).Result()
	if err != nil {
		return Reservation{}, "", err
	}
	if len(h) == 0 {
		return Reservation{}, "", ErrNotFound
	}
	expMs, _ := strconv.ParseInt(h["exp"], 10, 64)
	return Reservation{
		ID: reservationID, DropID: s.opts.DropID, UserID: h["user"], UnitID: h["unit"],
		Bucket: b, ExpiresAt: time.UnixMilli(expMs),
	}, h["state"], nil
}

// Freeze stops a hold from expiring while payment is in flight. It fails with ErrExpired if the
// hold has already passed its expiry, before anything is written to Postgres.
func (s *Store) Freeze(ctx context.Context, reservationID string) (Reservation, error) {
	b, err := s.bucketOf(reservationID)
	if err != nil {
		return Reservation{}, err
	}
	res, err := freezeScript.Run(ctx, s.rdb,
		[]string{s.holdKey(b, reservationID), s.expiryKey(b)},
		reservationID, s.opts.Now().UnixMilli(),
	).StringSlice()
	if err != nil {
		return Reservation{}, fmt.Errorf("freeze script: %w", err)
	}
	switch res[0] {
	case "OK":
		user := res[2]
		// Keep the one-per-account marker for as long as payment is in flight.
		_ = s.casMarker(ctx, s.userKey(s.HomeBucket(user), user), reservationID, "persist", "", 0)
		return Reservation{ID: reservationID, DropID: s.opts.DropID, UserID: user, UnitID: res[1], Bucket: b}, nil
	case "EXPIRED":
		return Reservation{}, ErrExpired
	default:
		return Reservation{}, ErrNotFound
	}
}

// Unfreeze puts a frozen hold back on a timer, used when checkout fails before charging.
func (s *Store) Unfreeze(ctx context.Context, reservationID string, extra time.Duration) error {
	b, err := s.bucketOf(reservationID)
	if err != nil {
		return err
	}
	exp := s.opts.Now().Add(extra).UnixMilli()
	return unfreezeScript.Run(ctx, s.rdb,
		[]string{s.holdKey(b, reservationID), s.expiryKey(b)}, reservationID, exp,
	).Err()
}

// Release returns the unit of a hold to its bucket (declined payment, cancelled checkout).
func (s *Store) Release(ctx context.Context, reservationID string) error {
	b, err := s.bucketOf(reservationID)
	if err != nil {
		return err
	}
	_, err = s.releaseHold(ctx, b, reservationID, "release", 0)
	return err
}

// releaseHold returns RELEASED, EXPIRED or SKIP.
func (s *Store) releaseHold(ctx context.Context, b int, rid, mode string, nowMs int64) (string, error) {
	res, err := releaseScript.Run(ctx, s.rdb,
		[]string{s.unitsKey(b), s.holdKey(b, rid), s.expiryKey(b)},
		rid, mode, nowMs,
	).StringSlice()
	if err != nil {
		return "", fmt.Errorf("release script: %w", err)
	}
	if res[0] == "SKIP" {
		return res[0], nil
	}
	user := res[1]
	_ = s.casMarker(ctx, s.userKey(s.HomeBucket(user), user), rid, "delete", "", 0)
	s.rdb.SAdd(ctx, s.nonEmptyKey(), b)
	s.rdb.Del(ctx, s.soldOutKey())
	return res[0], nil
}

// Discard removes a hold WITHOUT returning its unit. Used when Postgres shows the unit already
// belongs to someone else (Redis handed it out twice after a failover): returning it would create
// the duplicate again.
func (s *Store) Discard(ctx context.Context, reservationID, userID string) error {
	b, err := s.bucketOf(reservationID)
	if err != nil {
		return err
	}
	if err := completeScript.Run(ctx, s.rdb,
		[]string{s.holdKey(b, reservationID), s.expiryKey(b)}, reservationID,
	).Err(); err != nil {
		return fmt.Errorf("discard: %w", err)
	}
	_ = s.casMarker(ctx, s.userKey(s.HomeBucket(userID), userID), reservationID, "delete", "", 0)
	return nil
}

// FrozenHold is a hold whose payment has started.
type FrozenHold struct {
	ReservationID string
	UserID        string
	FrozenAt      time.Time
}

// FrozenHolds lists frozen holds older than minAge. It scans keys, so it is meant for the
// reconciler, not the request path.
func (s *Store) FrozenHolds(ctx context.Context, minAge time.Duration) ([]FrozenHold, error) {
	cutoff := s.opts.Now().Add(-minAge).UnixMilli()
	var out []FrozenHold
	iter := s.rdb.Scan(ctx, 0, "inv:{"+s.opts.DropID+":*:hold:*", 1000).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		h, err := s.rdb.HMGet(ctx, key, "state", "user", "frozen_at").Result()
		if err != nil || h[0] != "FROZEN" {
			continue
		}
		at, _ := strconv.ParseInt(fmt.Sprint(h[2]), 10, 64)
		if at > cutoff {
			continue
		}
		rid := key[strings.LastIndex(key, ":hold:")+len(":hold:"):]
		out = append(out, FrozenHold{ReservationID: rid, UserID: fmt.Sprint(h[1]), FrozenAt: time.UnixMilli(at)})
	}
	return out, iter.Err()
}

// Complete removes a hold after a successful payment. The unit stays sold and the user's marker
// becomes permanent so they cannot reserve a second unit. It is idempotent, and it also restores a
// marker lost in a Redis failover: Postgres already says this user bought, so the marker is set
// unconditionally.
func (s *Store) Complete(ctx context.Context, reservationID, userID string) error {
	b, err := s.bucketOf(reservationID)
	if err != nil {
		return err
	}
	if err := completeScript.Run(ctx, s.rdb,
		[]string{s.holdKey(b, reservationID), s.expiryKey(b)}, reservationID,
	).Err(); err != nil {
		return fmt.Errorf("complete script: %w", err)
	}
	return s.rdb.Set(ctx, s.userKey(s.HomeBucket(userID), userID), soldMarkerPref+reservationID, 0).Err()
}

// ExpireDue releases every HELD hold whose expiry has passed, across all buckets. It returns how
// many holds were expired. Frozen holds are never in the expiry set, so they are untouched.
func (s *Store) ExpireDue(ctx context.Context, limitPerBucket int64) (int, error) {
	now := s.opts.Now().UnixMilli()
	expired := 0
	for b := 0; b < s.opts.Buckets; b++ {
		ids, err := s.rdb.ZRangeByScore(ctx, s.expiryKey(b), &redis.ZRangeBy{
			Min: "-inf", Max: strconv.FormatInt(now, 10), Count: limitPerBucket,
		}).Result()
		if err != nil {
			return expired, err
		}
		for _, rid := range ids {
			if err := ctx.Err(); err != nil {
				return expired, err
			}
			status, err := s.releaseHold(ctx, b, rid, "expire", now)
			if err != nil {
				return expired, err
			}
			if status == "EXPIRED" {
				expired++
			}
		}
	}
	return expired, nil
}

// Available returns the number of unreserved units across all buckets.
func (s *Store) Available(ctx context.Context) (int64, error) {
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.IntCmd, s.opts.Buckets)
	for b := 0; b < s.opts.Buckets; b++ {
		cmds[b] = pipe.LLen(ctx, s.unitsKey(b))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	var total int64
	for _, c := range cmds {
		total += c.Val()
	}
	return total, nil
}
