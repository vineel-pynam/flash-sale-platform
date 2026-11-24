package payment

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"reservation-service/internal/psp"
)

// WorkerConfig bounds the worker. Concurrency is the PSP bulkhead: at most this many charges are
// in flight from one worker, whatever the backlog.
type WorkerConfig struct {
	DropID       string // only jobs for this drop are leased (one server runs one drop)
	Batch        int
	Concurrency  int
	Lease        time.Duration // how long a leased command is hidden from other workers
	PollInterval time.Duration
	MaxAttempts  int // after this many leases the command is left to the reconciler
	RetryBase    time.Duration
	RetryCap     time.Duration
}

func DefaultWorkerConfig() WorkerConfig {
	return WorkerConfig{
		Batch: 64, Concurrency: 32, Lease: 30 * time.Second, PollInterval: 50 * time.Millisecond,
		MaxAttempts: 8, RetryBase: 500 * time.Millisecond, RetryCap: 30 * time.Second,
	}
}

// Worker consumes PaymentRequested commands from the outbox.
//
// In production these commands go through Kafka (docs/adr/0003). Here the worker leases rows
// straight from the outbox table, which gives the same contract as a consumer group: at-least-once
// delivery, redelivery after a crash (lease expiry), and no ordering across reservations.
type Worker struct {
	db  *pgxpool.Pool
	psp *psp.Client
	res *Resolver
	cfg WorkerConfig
	log *slog.Logger
}

func NewWorker(db *pgxpool.Pool, client *psp.Client, res *Resolver, cfg WorkerConfig, log *slog.Logger) *Worker {
	return &Worker{db: db, psp: client, res: res, cfg: cfg, log: log}
}

type command struct {
	outboxID      int64
	reservationID string
	attempts      int
	amountCents   int64
	paymentToken  string
	paymentState  string
}

// Run processes commands until ctx is cancelled. On cancellation it stops leasing, lets in-flight
// charges finish (bounded by their own timeouts) and returns.
func (w *Worker) Run(ctx context.Context) error {
	t := time.NewTicker(w.cfg.PollInterval)
	defer t.Stop()
	for {
		n, err := w.ProcessBatch(ctx)
		if err != nil && ctx.Err() == nil {
			w.log.Error("payment batch failed", "err", err)
		}
		if ctx.Err() != nil {
			return nil
		}
		if n > 0 {
			continue // backlog: keep going without waiting
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// ProcessBatch leases up to Batch commands and handles them concurrently. It returns how many
// commands were leased.
func (w *Worker) ProcessBatch(ctx context.Context) (int, error) {
	cmds, err := w.lease(ctx)
	if err != nil || len(cmds) == 0 {
		return 0, err
	}
	// Charges must not be cut off half way by shutdown; each is bounded by the PSP client timeout.
	work := context.WithoutCancel(ctx)
	g := errgroup.Group{}
	g.SetLimit(w.cfg.Concurrency)
	for _, c := range cmds {
		g.Go(func() error {
			w.handle(work, c)
			return nil
		})
	}
	return len(cmds), g.Wait()
}

func (w *Worker) lease(ctx context.Context) ([]command, error) {
	rows, err := w.db.Query(ctx, `
		WITH next AS (
			SELECT id FROM outbox
			WHERE event_id = $3 AND event_type = 'PaymentRequested' AND processed_at IS NULL AND available_at <= now()
			ORDER BY id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox o
		SET available_at = now() + $2::interval, attempts = o.attempts + 1
		FROM next
		WHERE o.id = next.id
		RETURNING o.id, o.aggregate_id, o.attempts,
		          COALESCE((SELECT p.amount_cents  FROM payments p WHERE p.reservation_id = o.aggregate_id), 0),
		          COALESCE((SELECT p.payment_token FROM payments p WHERE p.reservation_id = o.aggregate_id), ''),
		          COALESCE((SELECT p.state         FROM payments p WHERE p.reservation_id = o.aggregate_id), 'MISSING')`,
		w.cfg.Batch, w.cfg.Lease.String(), w.cfg.DropID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []command
	for rows.Next() {
		var c command
		if err := rows.Scan(&c.outboxID, &c.reservationID, &c.attempts, &c.amountCents, &c.paymentToken, &c.paymentState); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (w *Worker) handle(ctx context.Context, c command) {
	log := w.log.With("reservation_id", c.reservationID, "attempt", c.attempts)

	if c.paymentState != "PENDING" {
		// Already resolved (by an earlier delivery, a webhook or the reconciler), or no payment row
		// exists. Either way there is nothing to charge; a missing row must not poison the batch.
		if c.paymentState == "MISSING" {
			log.Error("payment command without payment row; skipped")
		}
		w.markProcessed(ctx, c.outboxID)
		return
	}

	ch, err := w.psp.Charge(ctx, c.reservationID, c.amountCents, c.paymentToken)
	if err != nil {
		// Outcome unknown (timeout) or call not made (rate limit, breaker open). Never mark failed
		// and never release the unit here: the card may have been charged.
		if c.attempts >= w.cfg.MaxAttempts {
			log.Warn("payment unresolved after max attempts; leaving to reconciler", "err", err)
			w.markProcessed(ctx, c.outboxID)
			return
		}
		delay := psp.FullJitter(c.attempts, w.cfg.RetryBase, w.cfg.RetryCap)
		if errors.Is(err, psp.ErrCircuitOpen) {
			delay = max(delay, 5*time.Second)
		}
		if _, err := w.db.Exec(ctx, `UPDATE outbox SET available_at = now() + $2::interval WHERE id = $1`,
			c.outboxID, delay.String()); err != nil {
			log.Error("reschedule failed; lease expiry will redeliver", "err", err)
		}
		return
	}

	outcome, err := w.res.Apply(ctx, c.reservationID, ch, &c.outboxID)
	if err != nil {
		// The charge happened but recording it failed. The lease expires, the command is redelivered,
		// the PSP returns the same charge for the same key, and Apply runs again.
		log.Error("apply result failed; will be redelivered", "err", err)
		return
	}
	log.Debug("payment resolved", "outcome", outcome)
}

func (w *Worker) markProcessed(ctx context.Context, id int64) {
	if _, err := w.db.Exec(ctx, `UPDATE outbox SET processed_at = now() WHERE id = $1`, id); err != nil {
		w.log.Error("mark processed failed", "outbox_id", id, "err", err)
	}
}
