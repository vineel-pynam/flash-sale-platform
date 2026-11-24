# Problem & Goals

## 1. Objective

We run a checkout platform for limited items: concert tickets, flash sales, product drops.

- Normal traffic: **2,000 requests/sec**
- When a drop opens: **500,000+ requests/sec within 30 seconds**, for 5–15 minutes, then it falls fast

**What went wrong last time:**

| Problem | What users saw |
|---|---|
| p99 latency went from 300 ms to **25 s** | the page hung |
| Database ran out of connections | errors |
| Some users were **charged but got no order** | lost money |
| The **same seat was sold twice** | two people, one seat |

**Our goal:** redesign the platform so it stays **correct, fast and available** during huge, sudden spikes, including ones we did not plan for.

**In order of importance:**
1. **Correct:** never sell more than we have, never charge twice, never charge without an order.
2. **Available:** 99.95% of requests get a proper answer.
3. **Fast:** p99 < 500 ms when holding an item.

## 2. Why the old design broke

The old design had 4 weak points. Under a 250× spike, each one turned into one of the problems above.

| Weak point | What happened | Which problem it caused |
|---|---|---|
| **One hot row** | every buyer updated the same stock row in Postgres. One row takes only ~200–500 updates/s, one after another, but ~10K/s wanted in. 10K requests waiting at ~400/s is **25 s** | p99 went from 300 ms to 25 s |
| **Running out of connections** | autoscaling added servers, and each new server opened its own database connections. Requests stuck behind the hot row and the PSP kept their connections busy, so the total went past Postgres's limit | database out of connections |
| **Retry storms** | slow answers made browsers and services retry, often several layers at once. One click could become many calls, all hitting the part that was already struggling | more load exactly when the system was slowest |
| **Thundering herd** (everyone at once) | millions of people clicked in the same second at the start, with nothing in front to make them wait in line. Any cache that expired also sent all its requests to the database at the same moment | the spike reached the database in full |

Two more gaps turned slowness into wrong results:

| Gap | What happened | Which problem it caused |
|---|---|---|
| **Charge first, save the order after** | the PSP was called inside the request, before the order was saved. A crash or timeout in between left money taken with no order. A retry without a key could charge again | charged but no order |
| **Nothing in the database stopped a double sale** | stock was checked and then written in two steps, and the database had no rule that a unit can be sold only once. Two buyers could both see the seat as free | same seat sold twice |

Each of these is fixed in our design: doc 04, and the README section "Each incident and what fixes it".

## 3. Goals, with numbers

| Goal | Target | How we know it's possible |
|---|---|---|
| **Peak traffic** | 500K+ requests/s at the start of a drop, with no errors | the CDN and the waiting room answer ~97%. ~15K/s reach our servers, and we're built for 30K/s (doc 02, points 3–6) |
| **Hold speed** | p99 < 500 ms | Hold uses Redis only: ~240 ms worst case, including 180 ms of travel (doc 02, point 11) |
| **Available** | 99.95% of Holds answered properly, per 30 days **and** per drop | load shedding answers "try again in X s" instead of timing out (doc 06, section 3; doc 07, section 5) |
| **Time to be ready** | planned drop: all servers ready 30 min before, so there's no wait at the start. Surprise spike: waiting room on within 10 s; a new server in seconds using spare room, 2–3 min if it needs a new machine | doc 02, point 10; ADR-0006 |
| **Payment result** | 99% in 10 s, all within 5 min; confirmation email 99% in 30 s | payment and confirmation run in the background (ADR-0004) |
| **Oversell** | **0**, always | Redis gives each unit once; the Postgres unique rule rejects any second sale before a charge (ADR-0002) |
| **Double charge, or charged with no order** | **0**, always | the order is saved before the charge; one PSP key per reservation (ADR-0008) |
| **Data lost (RPO), back in (RTO)** | one availability zone fails: nothing lost, back in < 1 min. Whole home region fails: up to ~5 s of saves, no money lost, back in < 15 min | doc 06, section 7 |

**RPO** (Recovery Point Objective) = how much data we can lose. **RTO** (Recovery Time Objective) = how long until we're back.

## 4. What must never fail, and what can slow down

**Must never fail.** Each rule is protected in 3 layers: one layer stops the mistake, a second layer rejects it if the first one slips, and a check finds anything that got through.

| Rule | 1. Stops it | 2. If that slips | 3. Check that finds it |
|---|---|---|---|
| **A unit is sold only once** | Redis gives each unit ID to one person | the Postgres rule `PRIMARY KEY (event_id, unit_id)` rejects a second sale, before any charge | oversell check every 10 s |
| **One unit per person** | Redis checks "this person has no hold yet" | the Postgres rule `UNIQUE (event_id, user_id)` | the same oversell check |
| **A card is charged only once** | the PSP key = reservation ID, so a retry gets the first result | one payment per reservation (`UNIQUE (reservation_id)`); a state changes only if it's still the one we expect | double-charge check every 1 min, which refunds the extra |
| **No charge without an order** | the order is saved before the charge | the fix-up job compares PSP charges with orders every 1 min, and creates the order or refunds | the PSP's daily settlement report, checked line by line |

**Can slow down** (allowed to be late or less exact, so the rules above never break):

| What | How much | Why it's fine |
|---|---|---|
| "How many left" | may be 1–2 s old | it's a hint; the Hold gives the real answer |
| Queue order | random among everyone who joins up to 30 s after the start, then first come, first served | fairer than milliseconds deciding; bots gain nothing by being fastest |
| Payment result | the user sees "processing payment" for 1–3 s | the card is charged safely in the background |
| Confirmation email | up to 30 s | not needed in the same second |
| Under overload | "try again in X s", browse pages cut first, Pay cut last | a quick "try again" is better than a page that hangs |
| Whole home region down | the drop pauses for up to 15 min | a pause is better than selling a seat twice |

**Rule when in doubt:** pause, don't guess. Stopping is always safe; selling or charging wrong is not.
