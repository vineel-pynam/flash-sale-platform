# Testing & Observability

## 1. Load testing

**Where:** a production-sized copy (staging), with k6 load generators in 3 regions, so traffic comes through the CDN like real users.

**What we fake:**
- **PSP:** a simulator with the real limits: 1,000 payments/s, usually 0.4 s, sometimes 2 s, ~0.5% timeouts, respects idempotency keys. It can be made slow or failing during a test.
- **Waiting room:** the vendor's own load-test mode.
- **Users:** 10M test accounts, a 50,000-unit drop.

| Test | What we do | What it proves |
|---|---|---|
| **Spike** | 2K → 750K req/s in 30 s, hold, drop (1.5× the brief's 500K; the CDN answers most, our servers see ~22K) | the brief's scenario, with margin |
| **Soak** | steady 6K req/s (~3× a normal day) for 8 h, a small drop every hour | nothing slowly leaks (memory, connections, backlog) |
| **Stress** | +10% load every 2 min until it breaks | where the real limit is; shedding starts as planned |
| **Break one dependency** | weaken Redis, Postgres, Kafka or the PSP step by step | each fails the way we documented |
| **Surprise spike** | no pre-warm, 2K → 300K in 30 s | the waiting room (normally off on quiet days) switches on in < 10 s; autoscaling catches up |

**A test passes only if it's fast AND correct.** A fast test that oversells is a failed test:
- Hold p99 < 500 ms, and ≥ 99.95% of let-in Holds get a proper answer
- units sold ≤ 50,000
- 0 double charges, 0 charged without an order, 0 people with 2 units
- Postgres connections ≤ 80% of the limit
- memory and goroutines flat after the soak test

## 2. Chaos testing

We break things **during the spike**, because that's when backup paths run, and when they're most likely to be wrong. Tools: AWS FIS / Chaos Mesh (kill servers, or a whole availability zone), Toxiproxy (slow or cut Redis and Postgres connections), the PSP simulator's dials.

| We break | Pass if |
|---|---|
| Kill a Redis primary | its copy takes over in < 15 s; only that shard's Holds pause; no oversell |
| Redis failover that loses a hold | the unit goes to a 2nd person → Postgres rejects it → that person isn't charged |
| Redis loses all its data | Holds pause, Redis is rebuilt from Postgres, selling resumes; nothing sold twice, paid orders still paid |
| PSP +2 s slower for 3 min | no double charge; every order settled within 5 min |
| PSP "charged but timed out" (5%) | 0 charged without an order after 5 min |
| PSP says "too many requests" | our PSP calls stay ≤ 1,000/s (no retry storm) |
| Postgres primary fails over | nothing lost; no hold expires because of our outage |
| Kill a Kafka broker | no message lost; catches up in < 2 min |
| Kill payment workers mid-charge | 0 double charges |
| Cut off one availability zone | let-in users still served within the SLO |
| Waiting room down | gateway caps traffic at 30K/s; no oversell |
| Kill the hold expiry job | alert within 10 s; no oversell |
| Home region fails (on staging, practised every quarter) | the standby region takes over in < 15 min; no money lost; charges with no order are rebuilt from the PSP's records |

## 3. Game day

Chaos tests check the system. Game days check the **people**.
- **When:** 7 days before every big drop, and monthly. The home region failover (section 2) is practised every quarter.
- **Roles:** incident lead (decides), comms (users, status page), note-taker, and one owner per area: CDN and waiting room, our services, data (Redis, Postgres, Kafka), payments (PSP).
- **How:** run the spike on staging with the real dashboards, alerts and runbook; break 2–3 things **without warning**.
- **Measure:**
  - time to notice: < 2 min
  - time to the first right action (for example, pause admissions): < 5 min
  - time to recover: within the time in doc 06, section 7 for that failure
  - user message out: < 5 min
  - correctness after: oversell and double charges still 0
- **After:** a no-blame review the same day; every gap becomes a ticket fixed before the drop.

## 4. Logs, metrics, traces

We use 3 tools. Each answers a different question:

| Tool | Question it answers | What it records | Example | Rules |
|---|---|---|---|---|
| **Metrics** (Prometheus) | **is something wrong?** | numbers over time: requests/s, errors/s, p99, queue size | "Hold errors jumped from 0.01% to 2% at 10:03" | split only by things with few values (drop, service, outcome). Never by user or order ID: each value makes a new series, and 10M users would make 10M series and break it |
| **Traces** (OpenTelemetry) | **where is it slow?** | one request's path through every step, with the time of each | "Pay took 900 ms: our code 5 ms, Postgres 890 ms" | the trace ID is passed along at every step, including inside Kafka messages, so it continues into the payment worker and the PSP call. Keep all error and slow traces, 1% of the rest |
| **Logs** | **what happened to this one request?** | one JSON line per request: trace ID, drop, reservation ID, order ID, outcome, time | support searches reservation `41-abc` and sees: held 10:00:01, paid 10:00:40, PAID 10:00:42 | never card data. Keep all errors and payment events, 1% of normal reads. The trace ID links each log line to its trace |

**Traces vs logs:** a trace shows **where the time went** (each step and how long it took); a log shows **what happened** (IDs, results, error messages). They share the trace ID, so you can jump from one to the other. Used together: a metric says "Pay is slow", a trace shows it's Postgres, and the logs say why.

**Most important signals (top of the drop dashboard):**
1. **Correctness:** oversell, double charges, charged without an order. Must always be 0.
2. **Hold results by type:** OK / SOLD_OUT / ALREADY_HELD / ERROR. Only ERROR is bad; SOLD_OUT is a correct answer.
3. **Queue depth and age:** how many are waiting and how long the oldest has waited (waiting room, Kafka, outbox). Age matters more.
4. **Saturation:** how full each part is vs its limit (requests in progress, connection pools, Redis CPU per shard, PSP calls).
5. **Error budget burn:** how fast we're using up allowed failures (section 6).

## 5. SLIs and SLOs

- **SLI (Service Level Indicator):** what we measure, as good ÷ total. Example: 999,700 good Holds out of 1,000,000 = 99.97%.
- **SLO (Service Level Objective):** the target for that number.

| SLI | SLO |
|---|---|
| Hold answered properly. **Good:** `201` held, `200` already held, `410` sold out, `409` a click already in progress. **Bad:** `5xx` or no answer in time. People not yet let in by the waiting room don't count | 99.95% per 30 days **and** per drop |
| Hold faster than 500 ms | 99% (the brief's p99 < 500 ms) |
| Payment finished (PAID or FAILED) | 99% in 10 s, 100% in 5 min, not counting time the PSP itself is down (tracked separately) |
| Order confirmed + email | 99% in 30 s |
| Pages and waiting room served | 99.99% |
| Oversell, double charge, charged without an order | **always 0**; any one is an incident |

- **Error budget** = 100% − SLO: the failures we're allowed. For 1M Holds in a drop, 500 may fail. If the budget runs out, we stop risky changes (deploys, experiments) and fix reliability first.
- **Why per drop too:** a 30-day average can hide one bad 10-minute drop, because quiet days pull the average up.

## 6. Alerts without alert fatigue

If alerts fire for things that don't matter, people start ignoring them and miss the real one. So every alert must mean "users are hurting, act now".

**Error budget:** the SLO allows 0.05% of Holds to fail (99.95% must succeed). Those allowed failures are the error budget; in a drop of 1M Holds, 500 may fail. With budget left we can take risks (deploys); when it's used up, we stop risky changes and fix reliability first.

**What we alert on:**

| Type | When | Examples |
|---|---|---|
| **Page** (wake someone now) | users are being hurt right now, or money or stock is at risk | any oversell or double charge; charged without an order; error budget burning fast (6× or 14×); during a drop, > 1% of Holds failing in 2 min; payments stuck > 2 min |
| **Ticket** (fix in working hours) | something is slowly wrong, but users aren't hurt yet | budget burning a bit too fast over days (1×); a disk filling slowly; a certificate expiring soon; one copy of Redis or Postgres down while the system is still fine |
| **Dashboard only** (no alert) | normal signals used to find causes | traffic, CPU, memory, server count, queue sizes during a drop |

**Rules:**
1. **Alert on what users feel, not on causes.** High traffic and CPU are normal in a drop; paging on them would wake people every drop. They can also mislead: last time CPU was low while users waited 25 s. So we page on failed or slow requests and correctness, and keep the causes on the dashboard.
2. **Use ratios, not counts.** 500 errors/min is 0.06% during a drop but 50% on a quiet night. A planned 250× spike doesn't change a ratio, so a healthy spike causes no alert.
3. **Burn-rate alerts.** **Burn rate** = how fast we're spending the error budget.
   - **1×** = 0.05% failing → the budget lasts exactly 30 days → fine.
   - **6×** = 0.3% failing → gone in ~5 days → **page**.
   - **14×** = 0.7% failing → gone in ~2 days → **page**.
   - Slowly over budget for days → **ticket**.
   - **Two windows:** we page only if both a long window (for example 1 h) and a short one (5 min) are bad. The long one proves it's real, not a blip; the short one proves it's still happening, so the alert stops once fixed.
   - **Drop rule:** a drop lasts ~10–12 min, too short for a 1 h window, so during a drop we also page if more than 1% of let-in Holds fail in any 2 min.
4. **During a drop:** capacity warnings go to the drop chat channel, not to pages. **Correctness alerts always page.**
5. **Group related alerts:** one Redis failover = one page, not 50.
6. Every page links to its runbook step. Target: fewer than 2 pages per drop.

## 7. Catching problems in real time

Our design should prevent these problems; these checks are the safety net. They run all the time, so if a bug gets through, we find it in seconds or minutes, not from a customer. **No extra job is needed:** the fix-up job and the hold expiry job already do this work, and they also publish what they find.

| Check | How we check | Done by | How often | If found |
|---|---|---|---|---|
| Oversell | units sold in Postgres > stock | fix-up job (fast loop) | 10 s | pause admissions, page |
| Double charge | a hold with more than one successful charge at the PSP | fix-up job | 1 min | refund the extra, page |
| Charged without an order | a PSP charge with no matching order in Postgres | fix-up job | 1 min | create the order if the unit is free, otherwise refund |
| Payment with no answer | a payment still PENDING after 2 min | fix-up job | 1 min | ask the PSP and settle it |
| Hold frozen with no order | a hold frozen for Pay, but no order after 2 min (Checkout crashed) | fix-up job | 1 min | unfreeze it |
| Hold expiry job behind | the oldest expired hold hasn't been returned within 5 s | hold expiry job (reports on itself) | every run | alert; also alert if it stops reporting for 10 s |
| Redis and Postgres disagree | for example PAID in Postgres but still frozen in Redis | fix-up job | 1 min | repair Redis from Postgres |
| Our records vs the PSP's | the PSP's daily settlement report, line by line | fix-up job | daily | fix any difference |

Every run publishes what it found as a metric (for example `oversell_units = 0`), so the dashboard shows it live and the alerts in section 6 watch it. Every count should stay at 0.

## 8. Runbook for a live incident

A **runbook** is the step-by-step guide on-call follows during an incident. Everything is decided in advance, so nobody has to invent a plan under pressure.

**Switches:** ready-made controls, each a flag or one command. In an emergency we flip a switch instead of writing code or deploying. Every switch is practised at game days.

| Switch | Use when | What it does |
|---|---|---|
| **Pause admissions / change the let-in rate** | anything looks wrong with stock or payments | stops or slows new people coming in from the waiting room; people in the queue keep their place |
| **Slow-down levels** (Protect, Checkout only, Pause) | servers are filling up faster than the automatic switch reacts (doc 06, section 3) | sets the level by hand, turning off less important pages to keep buying working |
| **Force "sold out"** | the stock numbers can't be trusted (for example Redis and Postgres disagree badly) | the CDN shows "sold out" to everyone, so nobody else buys until it's checked |
| **Keep holds alive** (extend all holds, pause the hold expiry job) | people can't pay because of our outage (Postgres or the PSP down) | holds don't run out, so nobody loses their unit because of us |
| **Pause PSP calls** | the PSP behaves strangely (for example slow or charging oddly) | stops all calls to the PSP at once; payment jobs wait safely in Kafka |
| **Manual scale-up** | traffic grows faster than autoscaling adds servers | adds servers by hand. Gateway and Hold servers use no database connections, so add them freely; for Checkout and workers, check the database connection limit first |
| **Switch to the standby region** | the whole home region is down | the steps in doc 06, section 7 |

**First 5 minutes:**

| Step | What to do | Why |
|---|---|---|
| 1 | Is a strict rule at risk (oversell, double charge)? **Pause admissions now**, then investigate | stopping is always safe; selling or charging wrong is not |
| 2 | Find which part is failing, outside in: CDN → our services → data (Redis, Postgres, Kafka) → PSP | the dashboard is laid out in this order, so you check each layer top to bottom |
| 3 | Use a switch. Prefer slowing the queue over failing requests | waiting in the queue is a small annoyance; errors make people retry and add load |
| 4 | Tell users within 5 min, then every 15 min | silence makes people refresh and retry, which adds even more load |
| 5 | No deploys during the incident, unless the incident lead decides a fix must go out | a rushed deploy often makes things worse |

**Telling people:**
- **Users:** a banner on the queue page, the status page and social media. After the drop, emails to the people affected (refunds, delayed payments); the fix-up job has the list.
- **Inside the company:** one channel, with a summary every 30 min, so engineers aren't interrupted with questions.
- **Messages are written in advance**, for example "Payment delayed, you will not be charged twice". Careful wording is hard under stress.

## 9. In the repo

| What | Where |
|---|---|
| Spike, soak, stress tests (k6) | `loadtest/spike.js`, `soak.js`, `stress.js`. Load is set with `PEAK_RPS`; a full rehearsal at 750K also needs many k6 machines and goes through the CDN |
| Pass rules built into the tests | k6 thresholds: Hold p99 < 500 ms, availability > 99.95%, Pay accepted > 99.9%, 99% of payments settled in 10 s. The soak test also fails if goroutines or memory keep growing |
| Correctness check after every run | `go run ./cmd/verify` prints PASS only if nothing is oversold, nobody has 2 units, nothing is lost |
| Concurrency test | 100,000 users each try twice (200,000 attempts) for 50,000 units → exactly 50,000 holds, 0 left |
| End-to-end tests | unreliable fake PSP → no lost orders, no double charges; Redis gives one unit to 2 people → the second is never charged; Redis loses everything → rebuilt from Postgres; late charge after close → refunded; expired hold → Pay rejected before any write |
| Observability in the prototype | JSON logs; `/debug/vars` shows goroutines and memory (used by the soak test). No Prometheus or tracing yet |
| Results | **spike** (311,272 requests, 5,000 units): Hold p99 **5.7 ms**, 100% of Pay accepted, verify **PASS**. **Soak** (4 min): goroutines +3, memory +3.4 MB, no leak. **Stress** (300 → 1,500 buyers/s): Hold p99 30 ms; Pay shed load at the top (~70% accepted, then retried) as designed; 0 oversold |
| Limits | tests drive one server, with no CDN or waiting room in front; the fake PSP (50 ms) is faster than a real one |
