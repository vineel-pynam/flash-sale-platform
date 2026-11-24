# Execution Plan

## 1. Rules for the rollout

These are the rules we follow while **moving** from the old system to the new one.

| Rule | What it means | Why |
|---|---|---|
| **1. Correct first, fast second** | the next drop must not repeat any of the last drop's 4 problems (25 s p99, database out of connections, charged without an order, a seat sold twice; doc 01), even if it's slower | a slow drop annoys people; a wrong one loses their money and our trust. Speed comes later, with v1 |
| **2. Each phase works on its own** | v0, v1 and v2 are separate steps, and each one leaves the platform better than before | if plans change after any phase (budget, priorities), the platform is still safe. No phase depends on the next one to work |
| **3. Every change sits behind a flag** | a flag is a config switch that turns new code on or off without a deploy. Every change has a tested way back | if something misbehaves during a drop, we switch it off in seconds instead of rushing a deploy |
| **4. No big drop without passing the checks** | each phase must pass the 4 checks in section 5 (shadow traffic, canary, load test, chaos and game day) before a big drop runs on it | an upcoming drop is never a reason to skip a check; that's how mistakes reach real users |

**The one exception to rule 3:** the Postgres unique rules ("one unit sold once", "one unit per person") have no flag and are **never removed**. They only block wrong sales and never slow down a right one, so there's never a reason to turn them off.

## 2. v0: Stop the damage (1 week, before the next drop)

Small changes inside the **existing** services. No Redis, no Kafka, no new services yet.

| # | Change | Fixes |
|---|---|---|
| 1 | Add unique rules on allocations: one owner per unit, one unit per person | double sell |
| 2 | Stock as **one row per unit** instead of one counter row. Each buyer takes "any free unit not locked by someone else" (`SKIP LOCKED`), so buyers get different rows at once and nobody waits | hot row, slow p99 |
| 3 | **Save the order before charging.** Don't hold a database connection while waiting for the PSP: save the order and close the transaction, call the PSP, then save the result. (The user still waits for the PSP in v0; moving it to the background is v1.) | charged without an order, DB connections used up |
| 4 | Idempotency keys: on Pay, and for the PSP charge (= reservation ID) | double charge |
| 5 | PSP: 2.5 s timeout, circuit breaker, retries with random waits; **never mark failed on a timeout** | charged without an order |
| 6 | **Bought waiting room** in front of drop pages, with a fixed let-in rate set by on-call: ~500/s, lower than v1's 1,100/s because every Hold still uses the database in v0 | slow p99, DB overload |
| 7 | Bought bot protection at the CDN | bot load |
| 8 | **Connection limits:** PgBouncer; max servers × pool size ≤ 80% of Postgres's limit | DB connections used up |
| 9 | **Fix-up job** every minute: PSP charges vs orders, then create the order or refund. Run it once on the last drop to refund anyone still affected | charged without an order |
| 10 | Autoscale on requests (not CPU); manual pre-warm; no deploys around drops | slow scale-up |
| 11 | Browser: random retry waits, follow "try again in X s", disable the Pay button while paying | retry storms |
| 12 | Drop dashboard with the correctness counts; page on oversell or charged-without-order | slow detection |

**Cost:** all units are held in ~100 s instead of ~50 s (50,000 units ÷ ~475 Holds/s at the lower let-in rate). A correct, slower drop is fine.

## 3. v1: The new design (4–5 weeks, including 2 weeks of shadow mode)

Build what's in `04-architecture.md`: Hold off the database, the PSP out of the user's request.

| Work | What |
|---|---|
| Hold service | Redis Cluster, unit buckets, Hold and freeze scripts, hold expiry job, rebuild from Postgres |
| Checkout + payments | Checkout service, outbox, managed Kafka, payment workers (rate limit, breaker); the browser checks the status until it shows PAID or FAILED |
| Admission controller | automatic let-in rate from PSP health and stock, replacing the manual rate |
| Fix-up checks | all checks in `07-testing-and-observability.md`, section 7, each with a metric |
| Platform | automatic pre-warm from the drop schedule, spare room, graceful shutdown, canary releases |
| Monitoring | SLOs, burn-rate alerts, drop mode (the faster alert rules during a drop, doc 07, section 6), traces through Kafka |
| Testing | PSP simulator, k6 tests, chaos tests, first game day |

**Switching over safely** (the v0 path stays as a fallback):
1. **Shadow mode:** for normal sales, every Hold also runs through the new Redis path in the background; users only see the old answer. We compare the **results** (held / already held / sold out), not the unit IDs (each path picks different units), and check that the totals of units held and left agree. Target: ≥ 99.99% match for 2 weeks, every difference explained (the same as section 5).
2. **Small drop first** (~1,000 units), with v0 one flag away.
3. **Big drop** only after all checks in section 5 pass.

## 4. v2: Stronger (next 6 weeks)

v2 removes the single points of failure left after v1 (one region, one PSP, one CDN). Each item is separate and optional.

| Change | Why |
|---|---|
| Home region per drop + a practised standby region | survive a region failure in < 15 min (doc 06, section 7) |
| Let-in rate that learns from real payment speed | v1 uses a fixed formula (80% of the PSP limit ÷ the 71% who pay). v2 measures how fast payments really clear and how many really pay, and adjusts to keep the payment queue short: faster on a good day, slower sooner on a bad one |
| Our own bot signals (account age, past orders, cards or devices shared by many accounts) on top of the vendor's | catch abuse specific to us |
| Outbox sender on CDC instead of polling | less load on Postgres, only if the outbox gets much busier (doc 05) |
| A second PSP | no single payment provider can stop a drop; double the payment capacity. Each payment stays with the one PSP chosen when its order is saved, so a retry can never charge at the other one |
| A second CDN with DNS failover | no single CDN can stop a drop |
| Split Postgres by drop | only if many big drops run at the same time |
| Cards that need an OTP (3-D Secure), for countries that require it, such as India | an extension to the core flow: one browser step before Pay; the background charge and retries stay the same (doc 04, section 3.3) |

## 5. Checks before a big drop

Each phase passes 4 checks, in order. A failed check stops the rollout; it isn't skipped because a drop is close.

| Check | How | Pass if |
|---|---|---|
| **1. Shadow traffic** | copy real requests to the new path, with no charges and no user-visible changes; compare results (held / already held / sold out), not unit IDs | ≥ 99.99% match for 2 weeks; every difference explained |
| **2. Canary** | 1% → 10% → 50% → 100% of normal traffic, ≥ 30 min per step, automatic rollback; then one small live drop | errors and p99 no worse; correctness counts 0 |
| **3. Load test** | spike at 1.5× peak (750K req/s), plus soak and stress, on staging | every pass rule in `07-testing-and-observability.md`, section 1 |
| **4. Chaos + game day** | break things during the spike, with the real on-call team | every test passes; noticed in < 2 min; right action in < 5 min |

**For v0 (only 1 week):** check 1 replays the last drop's traffic on staging; check 2 is one internal test drop; check 3 is that replay at 1.5×; check 4 is a short chaos set (PSP slow, Postgres failover) with the on-call team.

**Go / no-go, 24 h before each big drop:**
- all checks passed for the version in production
- pre-warm dry run passed in the last 7 days
- the PSP knows the drop date and our expected payment rate
- switches tested at the last game day; on-call team and comms lead confirmed
- error budget not used up (if it is, the drop runs with a lower let-in rate, like v0's ~500/s)

## 6. Timeline

| Weeks | Phase | Drops run on |
|---|---|---|
| 1 | v0 build + replay the last drop on staging | the next drop runs on **v0** |
| 2–4 | v1 build. The Hold path comes first (weeks 2–3), so shadow mode can start on it while checkout and payments are finished | v0 |
| 4–5 | shadow mode on the Hold path (2 weeks of real traffic) | v0 |
| 6 | canary (1% → 100% of normal traffic), one small drop on v1, full load test, game day | v0 for big drops; the small drop on v1 |
| 7 | first big drop on v1 | **v1** (v0 one flag away) |
| 8–13 | v2, one item at a time | v1 |

## 7. Risks

| Risk | Impact | What we do |
|---|---|---|
| Old duplicate rows block the new unique rules | delays v0 | find them first: keep one buyer per seat, refund and email the others, then add the rules |
| The waiting room vendor isn't set up in time for the next drop | v0 runs without a queue | sign the contract in week 1; if it's still not ready, the gateway's traffic cap (30K req/s) protects our servers and the rest get "try again" |
| The real PSP limit is lower than we assumed | slower sell-out | confirm with the PSP in week 1; only the let-in rate changes |
| Waiting room problem on the day | drop outage | internal test drop; the gateway's traffic cap is the fallback |
| Redis and Postgres disagree in v1 | some failed Holds, never oversell | shadow mode, fix-up job; Postgres stays the truth |
| v1 takes longer | drops stay on v0 longer | v0 is safe on its own; v1 parts ship one by one |
| Users confused by "processing payment" | more support | test the wording on small drops; the wait is 1–3 s |
| On-call doesn't know the new switches | slow response | game day before every big drop |

**Rollback:** v0 changes are separate flags (except the unique rules, which stay). v1 runs next to v0 and is chosen per drop, so rolling back means running the next drop on v0. Each v2 item is separate and optional.

**Mid-drop:** we never switch versions during a drop (holds live in Redis in v1 and in Postgres in v0, so switching could lose them). If v1 fails during a drop, we pause admissions, fix it or finish the drop paused, and run the next drop on v0.
