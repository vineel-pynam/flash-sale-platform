# Load tests (k6)

Three profiles from docs/07-testing-and-observability.md, section 1. Each one encodes its pass criteria as k6 thresholds, and every run is followed by a correctness check, because **a fast run that oversells is a failed run.**

| Script | Shape | What it proves |
|---|---|---|
| `spike.js` | Normal → peak in 30 s (250×), hold, fall off. Reads and buyers mixed 50:1 | The brief's scenario: Hold p99 < 500 ms, availability ≥ 99.95%, payments resolved |
| `soak.js` | Steady moderate load for 20 min (8 h for a real soak) | No goroutine or heap growth, no latency drift, no backlog creep |
| `stress.js` | Buyers +10% every 2 min until Hold misses its p99 < 500 ms target (the test stops on its own) | The real capacity knee of one instance, to confirm assumption A8 (2,000 req/s per server) |

`lib.js` holds the buyer journey (Hold → Pay → check the result), admission-token signing (standing in for the waiting room) and the custom metrics the thresholds use.

## Running locally

```bash
cp .env.example .env            # then set real local values
make up                         # Redis + Postgres
make run DROP=spike-1 UNITS=5000   # API on :8080; saves the drop in Postgres, loads Redis from it (terminal 1)

make loadtest-spike DROP=spike-1   # terminal 2: laptop scale, 2,000 rps peak
make verify DROP=spike-1           # correctness check; exits 1 on any violation
```

Sales are kept in Postgres, so a sold-out drop stays sold out. Use a new `DROP` name for each fresh run. Without `make`, set `DROP_ID` yourself: `DROP_ID=spike-1 k6 run loadtest/spike.js`.

For `stress.js`, seed a drop large enough that it does not sell out during the ramp, otherwise the test ends up measuring the cheap sold-out path:

```bash
make run DROP=stress-1 UNITS=5000000
DROP_ID=stress-1 k6 run -e START_RPS=200 -e MAX_RPS=20000 loadtest/stress.js
```

## Reading the results

| Metric | Threshold | Meaning |
|---|---|---|
| `http_req_duration{name:reserve}` p99 | < 500 ms | Hold speed target |
| `reservation_availability` | > 99.95% | Share of Holds with a proper answer (held, already held, sold out) |
| `payment_accepted` | > 99.9% | Pay requests that got `202` within 3 tries (same key, following `Retry-After`) |
| `payment_resolved_10s` | > 99% | Payments that reached PAID or FAILED within 10 s |
| `http_req_failed{name:availability}` | < 0.1% | Browsing stays healthy during the drop |
| `reserve_held`, `reserve_sold_out`, `reserve_shed`, `payment_shed` | reported | How the drop played out; the `_shed` counters count 503/409 answered with `Retry-After` |
| `cmd/verify` | `RESULT: PASS` | Oversold = 0, no user with two units, no allocation for a failed order, units conserved |

## Scale

These scripts drive **one service instance**. The edge (CDN, waiting room) is not in the loop, so `browse` traffic hits the service's 500 ms in-process cache rather than a CDN.

The production rehearsal (docs/07-testing-and-observability.md section 1, docs/08-execution-plan.md section 5) runs the same scripts on distributed k6 runners in three regions against production-sized staging, at `PEAK_RPS=750000` (1.5× the brief's peak) with the waiting room in front. The rates in each script scale from `PEAK_RPS`, so no script changes are needed between the two.
