// Package httpapi exposes the reservation and checkout endpoints.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"expvar"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"

	"reservation-service/internal/checkout"
	"reservation-service/internal/inventory"
)

func init() {
	expvar.Publish("goroutines", expvar.Func(func() any { return runtime.NumGoroutine() }))
}

type Config struct {
	DropID              string
	AdmissionKey        []byte
	MaxInflightReserve  int
	MaxInflightCheckout int
	ReserveDeadline     time.Duration
	CheckoutDeadline    time.Duration
}

type API struct {
	cfg      Config
	rdb      redis.UniversalClient
	inv      *inventory.Store
	checkout *checkout.Service
	ready    atomic.Bool

	sf       singleflight.Group
	availMu  sync.Mutex
	availVal int64
	availAt  time.Time
}

func New(cfg Config, rdb redis.UniversalClient, inv *inventory.Store, co *checkout.Service) *API {
	if cfg.ReserveDeadline == 0 {
		cfg.ReserveDeadline = 300 * time.Millisecond
	}
	if cfg.CheckoutDeadline == 0 {
		cfg.CheckoutDeadline = 2 * time.Second
	}
	a := &API{cfg: cfg, rdb: rdb, inv: inv, checkout: co}
	a.ready.Store(true)
	return a
}

// SetReady flips readiness; shutdown sets it false first so the load balancer drains this pod.
func (a *API) SetReady(v bool) { a.ready.Store(v) }

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	auth := func(h http.Handler) http.Handler { return withAdmission(a.cfg.AdmissionKey, a.cfg.DropID, h) }
	idem := func(h http.Handler) http.Handler { return withIdempotency(a.rdb, 24*time.Hour, h) }

	// Reserve and checkout get separate in-flight limits: a slow checkout path cannot starve
	// reservations, and a reservation burst cannot starve buyers who are already paying.
	reserve := withInflightLimit(a.cfg.MaxInflightReserve,
		withDeadline(a.cfg.ReserveDeadline, auth(idem(http.HandlerFunc(a.reserve)))))
	pay := withInflightLimit(a.cfg.MaxInflightCheckout,
		withDeadline(a.cfg.CheckoutDeadline, auth(idem(http.HandlerFunc(a.submitPayment)))))
	get := withDeadline(a.cfg.ReserveDeadline, auth(http.HandlerFunc(a.getReservation)))

	mux.Handle("POST /v1/drops/{drop}/reservations", reserve)
	mux.Handle("POST /v1/reservations/{id}/payment", pay)
	mux.Handle("GET /v1/reservations/{id}", get)
	mux.HandleFunc("GET /v1/drops/{drop}/availability", a.availability)
	// Runtime stats (goroutines, heap) for soak tests: flat lines mean no leaks. In production this
	// is a Prometheus endpoint on an internal port, not the public listener.
	mux.Handle("GET /debug/vars", expvar.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !a.ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

type reservationResponse struct {
	ReservationID string    `json:"reservation_id"`
	UnitID        string    `json:"unit_id,omitempty"`
	ExpiresAt     time.Time `json:"expires_at,omitzero"`
	State         string    `json:"state"`
}

func (a *API) reserve(w http.ResponseWriter, r *http.Request) {
	if r.PathValue("drop") != a.cfg.DropID {
		writeError(w, http.StatusNotFound, "unknown_drop", "no such drop")
		return
	}
	res, err := a.inv.Reserve(r.Context(), userFrom(r.Context()))
	var held *inventory.AlreadyHeldError
	switch {
	case err == nil:
		writeJSON(w, http.StatusCreated, reservationResponse{
			ReservationID: res.ID, UnitID: res.UnitID, ExpiresAt: res.ExpiresAt, State: "HELD",
		})
	case errors.As(err, &held):
		// One unit per account; a retry gets the existing reservation.
		writeJSON(w, http.StatusOK, reservationResponse{ReservationID: held.ReservationID, State: "ALREADY_HELD"})
	case errors.Is(err, inventory.ErrSoldOut):
		writeError(w, http.StatusGone, "sold_out", "no units available")
	case errors.Is(err, inventory.ErrBusy):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusConflict, "request_in_progress", "another reservation request is running")
	case errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusServiceUnavailable, "timeout", "reservation timed out, retry")
	default:
		writeError(w, http.StatusServiceUnavailable, "unavailable", "reservation failed, retry")
	}
}

type paymentRequest struct {
	PaymentToken string `json:"payment_token"`
}

func (a *API) submitPayment(w http.ResponseWriter, r *http.Request) {
	var req paymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.PaymentToken == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "payment_token is required")
		return
	}
	o, err := a.checkout.Submit(r.Context(), r.PathValue("id"), userFrom(r.Context()), req.PaymentToken)
	switch {
	case err == nil:
		writeJSON(w, http.StatusAccepted, o)
	case errors.Is(err, checkout.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "reservation not found")
	case errors.Is(err, checkout.ErrForbidden):
		writeError(w, http.StatusForbidden, "forbidden", "reservation belongs to another user")
	case errors.Is(err, checkout.ErrExpired):
		writeError(w, http.StatusGone, "expired", "reservation expired")
	case errors.Is(err, checkout.ErrUnitTaken), errors.Is(err, checkout.ErrAlreadyBought):
		writeError(w, http.StatusConflict, "unit_unavailable", err.Error())
	default:
		w.Header().Set("Retry-After", "2")
		writeError(w, http.StatusServiceUnavailable, "unavailable", "checkout failed, retry")
	}
}

func (a *API) getReservation(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	user := userFrom(r.Context())
	if o, err := a.checkout.Get(r.Context(), id); err == nil {
		if o.UserID != user {
			writeError(w, http.StatusNotFound, "not_found", "reservation not found")
			return
		}
		writeJSON(w, http.StatusOK, o)
		return
	}
	hold, state, err := a.inv.Get(r.Context(), id)
	if err != nil || hold.UserID != user {
		writeError(w, http.StatusNotFound, "not_found", "reservation not found")
		return
	}
	writeJSON(w, http.StatusOK, reservationResponse{ReservationID: id, UnitID: hold.UnitID, ExpiresAt: hold.ExpiresAt, State: state})
}

// availability is cached for 500 ms per pod and coalesced with singleflight, so any number of
// concurrent readers cost at most two Redis round trips per second per pod.
func (a *API) availability(w http.ResponseWriter, r *http.Request) {
	if r.PathValue("drop") != a.cfg.DropID {
		writeError(w, http.StatusNotFound, "unknown_drop", "no such drop")
		return
	}
	a.availMu.Lock()
	if time.Since(a.availAt) < 500*time.Millisecond {
		v := a.availVal
		a.availMu.Unlock()
		writeAvail(w, v)
		return
	}
	a.availMu.Unlock()

	v, err, _ := a.sf.Do("avail", func() (any, error) {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 200*time.Millisecond)
		defer cancel()
		n, err := a.inv.Available(ctx)
		if err == nil {
			a.availMu.Lock()
			a.availVal, a.availAt = n, time.Now()
			a.availMu.Unlock()
		}
		return n, err
	})
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "availability unavailable")
		return
	}
	writeAvail(w, v.(int64))
}

func writeAvail(w http.ResponseWriter, n int64) {
	w.Header().Set("Cache-Control", "public, max-age=1, stale-while-revalidate=5")
	writeJSON(w, http.StatusOK, map[string]string{"available": strconv.FormatInt(n, 10)})
}
