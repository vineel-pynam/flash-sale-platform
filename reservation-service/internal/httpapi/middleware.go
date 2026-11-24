package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"reservation-service/internal/admission"
)

type ctxKey int

const userKey ctxKey = 1

func userFrom(ctx context.Context) string {
	u, _ := ctx.Value(userKey).(string)
	return u
}

// withAdmission verifies the waiting-room token locally and puts the user in the context.
func withAdmission(key []byte, dropID string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := admission.Verify(key, r.Header.Get("X-Admission-Token"), dropID, time.Now())
		if err != nil {
			writeError(w, http.StatusUnauthorized, "not_admitted", err.Error())
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userKey, c.UserID)))
	})
}

// withDeadline bounds the whole request. The deadline flows through context into Redis, Postgres
// and the PSP client, so work stops everywhere once the caller has given up.
func withDeadline(d time.Duration, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), d)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// withInflightLimit sheds load once a fixed number of requests are in flight. Rejection is
// immediate (no queueing in memory) and carries a jittered Retry-After so rejected clients come
// back spread out (docs/06-scalability-and-fault-tolerance.md, sections 3 and 4). Separate limiters per route class act as bulkheads.
func withInflightLimit(limit int, next http.Handler) http.Handler {
	sem := make(chan struct{}, limit)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
			next.ServeHTTP(w, r)
		default:
			w.Header().Set("Retry-After", strconv.Itoa(2+rand.IntN(5)))
			writeError(w, http.StatusServiceUnavailable, "overloaded", "server busy, retry later")
		}
	})
}

// ---- idempotency ----

type storedResponse struct {
	State  string `json:"state"` // in_progress | done
	Hash   string `json:"hash"`
	Status int    `json:"status,omitempty"`
	Body   []byte `json:"body,omitempty"` // stored verbatim so a replay is byte-identical
}

type recorder struct {
	http.ResponseWriter
	status int
	buf    bytes.Buffer
}

func (r *recorder) WriteHeader(s int) { r.status = s; r.ResponseWriter.WriteHeader(s) }
func (r *recorder) Write(b []byte) (int, error) {
	r.buf.Write(b)
	return r.ResponseWriter.Write(b)
}

// withIdempotency makes POST requests safe to retry. The first request with a key runs; a
// completed key replays the stored response; a key still in progress gets 409 so two concurrent
// retries never both execute; the same key with a different body gets 422.
func withIdempotency(rdb redis.UniversalClient, ttl time.Duration, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Idempotency-Key")
		if key == "" || len(key) > 128 {
			writeError(w, http.StatusBadRequest, "idempotency_key_required", "Idempotency-Key header is required")
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad_body", err.Error())
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		sum := sha256.Sum256(append([]byte(r.Method+" "+r.URL.Path+"\n"), body...))
		hash := hex.EncodeToString(sum[:])
		rkey := "idem:" + userFrom(r.Context()) + ":" + key

		claim, _ := json.Marshal(storedResponse{State: "in_progress", Hash: hash})
		ok, err := rdb.SetNX(r.Context(), rkey, claim, 30*time.Second).Result()
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "unavailable", "idempotency store unavailable")
			return
		}
		if !ok {
			replay(w, r, rdb, rkey, hash)
			return
		}

		rec := &recorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		bg := context.WithoutCancel(r.Context())
		if retryable(rec.status) {
			rdb.Del(bg, rkey) // not a final outcome: let a retry with this key execute again
			return
		}
		done, _ := json.Marshal(storedResponse{State: "done", Hash: hash, Status: rec.status, Body: rec.buf.Bytes()})
		rdb.Set(bg, rkey, done, ttl)
	})
}

// retryable responses are not stored: 5xx, 429, and 409 "in progress" mean "try again", so caching
// them would make every retry with the same key fail forever.
func retryable(status int) bool {
	return status >= 500 || status == http.StatusTooManyRequests || status == http.StatusConflict
}

func replay(w http.ResponseWriter, r *http.Request, rdb redis.UniversalClient, rkey, hash string) {
	raw, err := rdb.Get(r.Context(), rkey).Bytes()
	if errors.Is(err, redis.Nil) {
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusConflict, "request_in_progress", "retry shortly")
		return
	}
	var s storedResponse
	if err != nil || json.Unmarshal(raw, &s) != nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "idempotency store unavailable")
		return
	}
	switch {
	case s.Hash != hash:
		writeError(w, http.StatusUnprocessableEntity, "idempotency_key_reused", "key was used with a different request")
	case s.State == "in_progress":
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusConflict, "request_in_progress", "a request with this key is still running")
	default:
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Idempotent-Replayed", "true")
		w.WriteHeader(s.Status)
		_, _ = w.Write(s.Body)
	}
}

// ---- responses ----

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]string{"error": code, "message": msg})
}
