package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"reservation-service/internal/testutil"
)

func withUser(r *http.Request, u string) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), userKey, u))
}

func TestIdempotency_ReplaysCompletedResponse(t *testing.T) {
	rdb := testutil.Redis(t, 8)
	var calls atomic.Int32
	h := withIdempotency(rdb, time.Minute, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSON(w, http.StatusCreated, map[string]string{"reservation_id": "r-1"})
	}))
	key := testutil.DropID("idem")

	send := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/drops/d/reservations", strings.NewReader(body))
		req.Header.Set("Idempotency-Key", key)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, withUser(req, "alice"))
		return rec
	}

	first := send(`{}`)
	second := send(`{}`)
	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", calls.Load())
	}
	if second.Code != first.Code || second.Body.String() != first.Body.String() {
		t.Fatalf("replay differs: %d %q vs %d %q", second.Code, second.Body, first.Code, first.Body)
	}
	if second.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatal("missing Idempotent-Replayed header")
	}
	if third := send(`{"different":true}`); third.Code != http.StatusUnprocessableEntity {
		t.Fatalf("reused key with new body: got %d, want 422", third.Code)
	}
}

func TestIdempotency_ConcurrentRetriesRunOnce(t *testing.T) {
	rdb := testutil.Redis(t, 64)
	var calls atomic.Int32
	h := withIdempotency(rdb, time.Minute, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		time.Sleep(100 * time.Millisecond)
		writeJSON(w, http.StatusAccepted, map[string]string{"ok": "1"})
	}))
	key := testutil.DropID("idem")
	var wg sync.WaitGroup
	codes := make(chan int, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{}`))
			req.Header.Set("Idempotency-Key", key)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, withUser(req, "alice"))
			codes <- rec.Code
		}()
	}
	wg.Wait()
	close(codes)
	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", calls.Load())
	}
	for c := range codes {
		if c != http.StatusAccepted && c != http.StatusConflict {
			t.Fatalf("unexpected status %d", c)
		}
	}
}

// A "try again" answer (409 in progress, 429, 5xx) must not be cached against the key, or every
// retry with that key would get the same failure forever.
func TestIdempotency_RetryableResponsesAreNotCached(t *testing.T) {
	rdb := testutil.Redis(t, 8)
	var calls atomic.Int32
	h := withIdempotency(rdb, time.Minute, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			writeError(w, http.StatusConflict, "request_in_progress", "busy")
			return
		}
		writeJSON(w, http.StatusCreated, map[string]string{"ok": "1"})
	}))
	key := testutil.DropID("idem")
	send := func() int {
		req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{}`))
		req.Header.Set("Idempotency-Key", key)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, withUser(req, "alice"))
		return rec.Code
	}
	if c := send(); c != http.StatusConflict {
		t.Fatalf("first: %d", c)
	}
	if c := send(); c != http.StatusCreated {
		t.Fatalf("retry after 409: got %d, want 201 (409 must not be cached)", c)
	}
}

func TestInflightLimit_ShedsWithRetryAfter(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 2)
	h := withInflightLimit(2, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-release
	}))
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil)) }()
	}
	<-started
	<-started

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	close(release)
	wg.Wait()
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("got %d (Retry-After %q), want 503 with Retry-After", rec.Code, rec.Header().Get("Retry-After"))
	}
}
