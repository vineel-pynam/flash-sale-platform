# ADR-0007: Let people in only as fast as the PSP can charge them

## Context

A **Hold** (reservation) is the first click: the buyer gets one specific unit kept for them for 3 min while they pay. If they don't pay in time, the unit goes back for someone else.

However many servers we run, the PSP charges only 1,000 cards/s (02-scale-and-numbers.md, A1). If we let people in faster than that, they hold units but wait too long at the payment step. Their holds expire, while people who could have bought are still in the queue. Letting people in when no units are free wastes their time and adds load, with no sale at the end.

## Decision

Our **admission controller** sets how fast the waiting room lets people in (the let-in rate):

- **PSP budget:** 800 new charges/s (80% of the PSP's 1,000/s; the other 200/s is kept for retries)
- **Share of people who reach payment:** 95% hold × 75% pay ≈ 71%
- **Let-in rate:** 800 ÷ 0.71 ≈ **1,100 people/s** (02-scale-and-numbers.md, point 5)
- **Never more people than free units:** if only 300 units are free, at most 300 more people are let in. When none are free, 0. It starts again as holds expire or cards are declined.

**Every 5 s** it works the rate out again and lowers it when:
- the PSP gets slower
- the circuit breaker on PSP calls opens
- the payment job backlog in Kafka grows

On-call can change the rate or pause it at any time.

## What we didn't pick

| Option | Why not |
|---|---|
| Let people in as fast as our servers can handle (~15,000/s) | our servers aren't the limit, the PSP is. Holds pile up waiting to pay and expire, wasting units and time |
| A fixed rate set before the drop | can't react if the PSP slows down, many cards are declined, or units run out faster than expected. Used only in v0 (~500/s, set by on-call; 08-execution-plan.md, section 2) |
| No waiting room, only a request limit | people are turned away at random instead of waiting in a fair queue, and every retry adds load |

## Consequences

- **Good:**
  - payments stay under the PSP's limit by design (1,100 × 71% ≈ 780/s), so the PSP never causes failures
  - the queue is honest: when no units are free, nobody is let in just to fail
- **Bad: a longer queue than our servers alone would need.** **Fallback:** we accept it. Those people couldn't have paid any faster.
- **Bad: the drop depends on the controller's numbers being right.** **Fallback:** a fixed minimum and maximum for the rate; an alert when the real let-in rate differs from the planned one; on-call can override it.

## When to revisit

- We get a higher PSP limit or add a second PSP. The formula stays; only the numbers change.
- The share of people who pay differs a lot between kinds of drops. Then use past drops of the same kind.
- v2: the controller learns how fast payments really clear and how many people really pay, instead of using fixed numbers (08-execution-plan.md, section 4).
