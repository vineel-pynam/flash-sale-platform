# Spike-Resilient Flash Sale & Ticketing Platform: Architecture Proposal

## Assumptions and Capacity Model

Every sizing decision in this document comes from the numbers below. Where the brief does not give a
number, I state an assumption and mark it **(A)**. If a real value differs (most importantly the PSP
rate limit), the derived numbers scale roughly linearly and the design does not change shape.

### Given by the brief

| Input | Value |
|---|---|
| Normal traffic | 2,000 rps |
| Drop traffic | 500,000+ rps, reached within 30 s, lasting 5 to 15 min |
| Users in the waiting room | up to 10M concurrent |
| Inventory for one hot item | 50,000 units |
| Peak read:write ratio | ~50:1 |
| Reservation path SLO | 99.95% availability, p99 < 500 ms |
| Payments | third-party PSP with its own rate limits and occasional timeouts |
| Users | multi-region |

### Added assumptions

| # | Assumption | Value | Why this value |
|---|---|---|---|
| A1 | PSP contracted rate limit | **1,000 payment authorisations/s** | Typical merchant-tier limit; the single most important input |
| A2 | PSP latency | p50 400 ms, p99 2 s, ~0.5% timeouts | Card networks plus 3DS |
| A3 | Purchase limit | 1 unit per account for the hot item | Standard for hype drops; per-seat for ticketed events |
| A4 | Hold TTL | 3 min to complete checkout; frozen while payment is in flight | Long enough to pay, short enough to recycle abandoned stock |
| A5 | Funnel | 95% of admitted users reserve; 75% of holders submit payment; 10% of payments decline | Conservative e-commerce funnel |
| A6 | Waiting room poll interval | 30 s per client | Keeps queue position fresh without becoming the spike |
| A7 | Regions | 3 (Americas, Europe, Asia); each drop's inventory lives in one home region | See ADR-0005 |
| A8 | Postgres | `max_connections` = 500 behind PgBouncer | Large managed instance |
| A9 | Go API pod | ~2,500 rps per 2 vCPU pod at target latency | To be confirmed by the prototype benchmark |
| A10 | Cold scale-out | 2 to 3 min (node provision 60 to 120 s, image pull ~20 s, start + readiness ~15 s) | Typical managed Kubernetes |

### C1. Traffic shape at peak

| Quantity | Calculation | Result |
|---|---|---|
| Reads at peak | 500K × 50/51 | **~490K rps** |
| Writes at peak (naive system, no admission control) | 500K × 1/51 | **~9.8K rps** |
| Waiting room polling alone | 10M users ÷ 30 s | **~333K rps** |
| Share of users who can ever succeed | 50K ÷ 10M | **0.5%** |

Two conclusions drive the design:

1. **About two thirds of the spike is people asking "is it my turn yet?"** That traffic must be answered
   at the edge and must never reach origin.
2. **99.5% of users will leave without the item no matter how big our servers are.** The job is not to
   serve 500K rps of purchases. It is to let the right number of people in, sell 50K units correctly,
   and tell everyone else quickly and cheaply.

### C2. The real bottleneck is the PSP, so it sets the admission rate

We cannot take payments faster than the PSP allows, so there is no point admitting buyers faster than
that either. Admitting more only creates holds that sit waiting on a payment queue and time out.

| Step | Calculation | Result |
|---|---|---|
| Payment budget for new attempts | 80% of 1,000/s (20% kept for retries and status lookups) | **800/s** |
| Admitted users who reach payment | 95% reserve × 75% pay | **~71%** |
| Admission rate from the PSP | 800 ÷ 0.71 | **~1,100 admissions/s** |
| Admission rate from stock | admit only while unreserved units > 0 | caps the above |

**Admission rate = min(PSP-derived rate, remaining unreserved stock).** The controller starts at
1,100/s and adjusts from live PSP latency and error rate (v2: fully adaptive).

### C3. How the drop plays out over time

| Phase | Calculation | Result |
|---|---|---|
| All 50K units held | 50K ÷ (1,100 × 0.95) | **~50 s after open** |
| Peak payment attempts | 1,045 holds/s × 75% | **~780/s**, under the 800/s budget |
| Sold in the first wave | 50K × 75% × 90% | **~33.7K (67%)** |
| Units returned (abandoned + declined) | 50K − 33.7K | **~16.3K**, re-offered to the queue |
| Remaining after waves 2, 3, 4 | ×0.325 per wave | ~5.3K → ~1.7K → ~0.6K |
| Effectively sold out | 3 to 4 waves of a 3 min TTL | **~T+10 to 12 min** |
| Total users ever admitted | ~50K ÷ 0.675, plus non-reservers | **~78K (0.8% of the queue)** |

The theoretical floor is 50K ÷ 800 ≈ 63 s. Real sell-out takes longer because of abandoned holds, which
matches the 5 to 15 minute spike in the brief. Declined payments release their unit immediately, so in
practice the tail is shorter than this model.

**Product consequence:** when available units hit 0 but holds still exist, users still in the queue
see "all units are currently reserved, you may still get one" rather than "sold out". We only show
"sold out" when available = 0 **and** active holds = 0.

### C4. What actually reaches origin

| Source | Calculation | Origin load |
|---|---|---|
| Waiting room polling | served by the edge waiting room | **0** |
| Catalog and availability reads | CDN, 1 s TTL, origin shield, request coalescing | **~1 req per object per second**, a few hundred rps |
| Admissions | from C2 | ~1.1K rps |
| Active checkout traffic | ≤ ~55K concurrent sessions × 1 request per 4 s | ~14K rps |
| **Expected origin peak** | | **~15K rps** |
| **Origin design capacity** | 2× expected | **30K rps** |

The edge absorbs about **97% of the spike**. Origin is sized for 30K rps, not 500K.

Reservation writes drop from ~9.8K rps (naive) to **~1.1K rps** (admitted). The difference is the
admission controller.

### C5. Compute sizing

| Item | Calculation | Result |
|---|---|---|
| API pods for 30K rps | 30K ÷ 2.5K (A9) | 12 pods |
| Survive the loss of 1 of 3 AZs | 12 × 1.5 | 18 pods |
| Pre-warmed for a scheduled drop | 18 + ~30% margin | **24 pods per hot-path service** |
| HPA ceiling | 2× pre-warm | **48 pods** |
| Time to add capacity cold | A10 | **2 to 3 min** |
| Time for the spike to arrive | brief | **30 s** |

Cold scale-out is 4 to 6 times slower than the spike. **Reactive autoscaling cannot save a drop.**
Capacity has to be there before the drop opens (scheduled) or the waiting room must hold traffic back
while it arrives (unscheduled). CPU is the worst signal of all, because it rises only after requests
are already queuing.

### C6. Redis (inventory gate)

| Item | Calculation | Result |
|---|---|---|
| Reserve operations at the admitted rate | C2 | ~1.1K/s |
| Including releases, expiry reaper, availability summary | | **< 5K ops/s** |
| Worst case: waiting room fails open or is not engaged yet | all writes from C1 | ~9.8K reserve/s |
| Single-key ceiling | single-threaded; Lua costs more than plain INCR | ~50K Lua calls/s per key |
| Buckets | 50 × 1,000 units over a 6-shard cluster | **~200 ops/s per bucket** in the worst case |

At the admitted rate a single key would cope. Bucketing exists for two other reasons:

- It protects the **fail-open** and **unscheduled spike** cases, before the waiting room engages.
- It spreads one hot item across **all cluster shards**, instead of pinning the whole drop to one Redis
  thread and one replica's failover blast radius.

### C7. Postgres (source of truth)

| Item | Calculation | Result |
|---|---|---|
| Orders at peak | bounded by successful payments | ≤ ~800/s |
| Writes per order | order + payment state + unit allocation + outbox row, in one transaction | ~4 rows |
| Peak transaction rate | | **~1K TPS, ~3K row writes/s** |

That is comfortably within a single primary. **No sharding is needed for one drop.** Orders are hash
partitioned by `order_id` for long-term growth and many drops, not for the spike itself.

**Connection budget.** This is the incident we actually had, so it is sized against the **HPA maximum**,
never the current replica count:

| Consumer | Max replicas × pool size | Connections |
|---|---|---|
| Reservation service (hot path) | does not touch Postgres | **0** |
| Order writer | 16 × 10 | 160 |
| Outbox relay | 3 × 4 | 12 |
| Reconciler | 2 × 5 | 10 |
| Admin, migrations, on-call headroom | | 20 |
| **Total** | | **202 of 400** (80% of 500) |

Rule: **Σ (max replicas × pool size) ≤ 80% of `max_connections`.** Autoscaling then physically cannot
exhaust the database.

### C8. Kafka (order event log)

| Item | Value |
|---|---|
| Event rate at peak | ~5K events/s (order, payment, allocation, notification) |
| Partitions | 48, keyed by `order_id` |
| Durability | RF = 3, `acks=all`, `min.insync.replicas=2` |

Partitioning by `item_id` would put the entire drop on one partition and one consumer. Partitioning by
`order_id` spreads it evenly while keeping each order's events in order.

### C9. Latency budget for the reservation path (p99 < 500 ms)

Measured from the edge receiving an admitted user's reserve request to the user receiving "you hold
the unit". Payment is **not** on this path and has its own SLO (p99 < 3 s, bounded by the PSP).

| Hop | p99 budget |
|---|---|
| Edge: TLS, WAF, admission token check | 20 ms |
| Edge to home region over persistent connections (worst case, intercontinental) | 180 ms |
| API gateway: auth, rate limit, priority class | 15 ms |
| Reservation service logic | 10 ms |
| Redis Lua reserve (up to 3 bucket probes) | 15 ms |
| **Total** | **240 ms** |
| **Headroom** (GC pauses, a single client retry, noisy neighbours) | **260 ms** |

The service makes **no synchronous database or PSP call** on this path. That is what makes the 500 ms
target achievable at all.

### C10. Availability and recovery targets

| Target | Value | Meaning |
|---|---|---|
| Availability | 99.95% | 21.9 min/month error budget; during a drop, ≤ 1 in 2,000 admitted reservation requests may fail |
| What counts as failure | 5xx or timeout on an **admitted** request | Waiting in the queue and intentional 429s to unadmitted or bot traffic are not failures |
| Oversell | **0** | Hard invariant |
| Charged without order | **0 after a 5 min reconciliation window** | Auto-fulfil or auto-refund |
| RPO: orders, payments | **0** | Synchronous replica in a second AZ |
| RPO: Kafka | **0** | `acks=all`, `min.insync.replicas=2` |
| RPO: Redis holds | **≤ 1 s** | Async replication; Postgres constraint and reconciliation cover the gap |
| RTO: AZ failure | **< 1 min** | Automatic failover |
| RTO: home region failure | **< 15 min** | Admissions paused, standby region promoted, admissions resumed |
| Scale-out time | **0 s** scheduled (pre-warmed); **< 10 s** to engage the waiting room for unscheduled spikes | Capacity follows in 2 to 3 min behind the queue |
