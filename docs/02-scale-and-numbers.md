# Scale & Numbers

## 1. Given by the brief

| Input | Value |
|---|---|
| Normal traffic | 2,000 req/s |
| Drop traffic | 500,000+ req/s, reached in 30 s, lasts 5–15 min |
| Users in the waiting room | up to 10 million at once |
| Stock for one hot item | 50,000 units |
| Reads : writes | about 50 : 1 |
| Target | 99.95% available, p99 < 500 ms to hold an item |
| Payments | outside provider (PSP), has rate limits and timeouts |
| Users | many regions |
| Spikes | partly planned (scheduled drops), partly surprise (viral posts, bots) |

## 2. Our assumptions

| # | Assumption | Value |
|---|---|---|
| A1 | PSP limit | **1,000 payments/s** (the most important number) |
| A2 | PSP speed | usually 0.4 s, slow ones 2 s. ~0.5% (1 in 200) get no answer in time; the card may still be charged |
| A3 | Buy limit | 1 unit per person |
| A4 | Hold time | 3 min to pay |
| A5 | User behaviour | 95% of let-in users hold, 75% of them pay, 10% of cards decline |
| A6 | Regions | 3 (Americas, Europe, Asia); each drop's stock lives in one "home" region |
| A7 | Availability zones | 3 per region; we keep running if one availability zone fails |
| A8 | One server (pod) | ~2,000 req/s |
| A9 | Adding a server that isn't pre-warmed | 2–3 min (surprise spikes, growing past 24, replacing a dead server) |
| A10 | Waiting room check | each browser asks "is it my turn?" once every 30 s |
| A11 | Time in checkout | ~50 s per person (hold, enter card, pay, wait for result), ~1 request every 4 s |
| A12 | PSP spare room | we use 80% of the PSP limit for new payments, and keep 20% for retries |

## 3. Back-of-envelope

**1. Who can actually buy?**
- 50,000 units ÷ 10M people = **0.5%**
- *Why it matters:* 199 of every 200 people will get "sold out". So our main job is to **say no fast and fairly**, and to sell the 50,000 correctly.

**2. Split the 500K requests/s**
- The brief says reads : writes = 50 : 1. So out of every 51 requests, **50 are reads** and **1 is a write**.
- Reads ≈ **490K/s** ("show the page", "is it my turn?", "how many left?")
- Writes ≈ **10K/s** (Hold / Pay clicks)

**3. "Is it my turn?" checks**
- 10M people in the waiting room, each browser asks once every 30 s
- 10M ÷ 30 ≈ **333K/s**
- *Why it matters:* that's two thirds of the spike, and it's only "wait" or "go". The **waiting room answers it at the edge**, so it never reaches us.

**4. Who answers what**

| Request | Per second | Who answers |
|---|---|---|
| "Is it my turn?" | ~333K | waiting room (edge) |
| Page views, "how many left?" | ~157K (490K − 333K) | CDN (same answer for everyone, cached 1 s) |
| Hold / Pay clicks | ~10K want in | only ~1,100/s are let in (see 5) |

**5. How fast do we let people in? ~1,100/s**
- The PSP takes 1,000 payments/s. We use **800/s** for new payments and keep 200/s for retries.
- Of the people let in, **~71%** reach payment (95% hold × 75% pay).
- 800 ÷ 0.71 ≈ **1,100 people/s**
- *Why it matters:* letting people in faster than we can charge them only makes them wait at the payment step, and their holds expire.

**6. What reaches our servers: ~15K/s**
- **New people:** ~1,100/s, each sends a Hold (from point 5)
- **People already inside:** after the Hold they open checkout, pay, and keep asking "paid yet?"
  - People inside at once = people in per second × time each stays = 1,100 × ~50 s ≈ **55K**
  - Each sends ~1 request every 4 s → 55K ÷ 4 ≈ **14K/s**
- Total ≈ 1.1K + 14K ≈ **15K/s**. We build for **30K/s** (2×), so even if people stay twice as long, we are fine.
- *Why it matters:* 15K of 500K = **3%**. The edge handles **97%**.

**7. How long does the sale last? ~10–12 min**
- We let in ~1,100 people/s, and 95% of them hold → **~1,050 holds/s**
- 50,000 units ÷ 1,050 per second ≈ **~50 s** until every unit is held
- But not every hold becomes a sale: 75% pay, and 90% of cards go through → about **2 in 3 sell**
- The other **1 in 3 come back**: declined cards right away, people who left after their 3-min hold ends
- Those units are offered again to the next people in the queue:

| Round | Units on offer | Sold (~2 in 3) | Come back |
|---|---|---|---|
| 1 | 50,000 | ~33,500 | ~16,500 |
| 2 | ~16,500 | ~11,000 | ~5,500 |
| 3 | ~5,500 | ~3,700 | ~1,800 |
| 4 | ~1,800 | ~1,200 | ~600 |

- Each round takes up to **3 min** (the hold time). 3–4 rounds ≈ **10–12 min**, which matches the brief's 5–15 min.

**8. How many servers? 24 ready, up to 48**
- We need **30K req/s**, and one server handles 2,000 → 30K ÷ 2,000 = **15 servers per busy service** (API gateway, Hold service). The checkout service is sized separately (see 9).
- We run in **3 availability zones** (3 separate data centres in the same region). One can fail (power, network).
- So the **other 2 availability zones alone** must still have 15 servers → **8 per availability zone** → 8 × 3 = **24 servers**
- If one availability zone goes down: 8 + 8 = **16 left**, still ≥ 15
- Autoscaling can add more, up to **48** (2× the 24). This is a safe ceiling. These servers only use Redis, so they add **no** database connections.

**9. Database: ~3,000 row writes/s, connections capped**
- **Writes:** at most ~800 orders/s (we can't charge faster than that), and each order saves 4 rows → **~3,000 rows/s**. One Postgres handles this easily.
- **Connections:** Postgres allows 500. Our rule: **max servers × connections each**, added up for every service, must be **≤ 400** (80%).
  - The Hold servers use **Redis only → 0** connections
  - Only the services that save orders and payments use Postgres:

| Service | Max servers × connections | Total |
|---|---|---|
| Checkout (Pay) | 24 × 5 | 120 |
| Payment worker | 16 × 4 | 64 |
| Order worker | 16 × 4 | 64 |
| Outbox sender + fix-up job | 3 × 4 + 2 × 5 | 22 |
| Spare (admin, emergencies) | | 20 |
| **Total** | | **290 ≤ 400** |

- **PgBouncer** in front of Postgres lets these connections share a smaller set of real database connections.
- *Why it matters:* last time, autoscaling opened too many connections and the database fell over. Because we count at **max** size, it **can't happen** now.

**10. Why pre-warm?**
- The spike arrives in **30 s**.
- A new server takes **2–3 min** to be ready: get a machine (1–2 min) + download our app (~20 s) + start and pass health checks (~15 s).
- So if we wait for traffic and then add servers, the servers arrive **after** the rush has already hit.

**What we do:**
- **Planned drop:** start all **24 servers ~30 min before** it opens. At opening time, nothing is cold.
- **Surprise spike** (viral, bots): the **waiting room switches on within 10 s** and lets people in only as fast as the current servers can handle. Meanwhile autoscaling adds servers (2–3 min), and the waiting room lets more people in as they come up.

**11. Time budget for a Hold: p99 < 500 ms**

| Step | Time (p99) |
|---|---|
| Edge: secure connection, firewall, check the entry pass | 20 ms |
| Travel from the user's nearest edge to the home region (worst case, other continent) | 180 ms |
| API gateway: login check, rate limit | 15 ms |
| Our Hold code | 10 ms |
| Redis: take a unit (may try up to 3 buckets) | 15 ms |
| **Total** | **240 ms** |
| **Spare** (slow moments, one retry) | **260 ms** |

- *Why it matters:* a Hold **never waits on the database or the PSP**. It only talks to Redis. That's what keeps it under 500 ms even at peak.

**12. Redis: small load, small memory**
- **Normal:** ~1,100 Holds/s. With payments, expiry and "how many left" counts, that's **under 5K Redis commands/s**.
- **Worst case:** the waiting room fails or isn't on yet → ~10K Holds/s.
- One Redis key handles ~**50K** of our Hold scripts per second, so even the worst case fits.
- **Memory:** 50,000 unit IDs + holds ≈ a few MB.
- **Why 50 buckets then?** Not for speed. They spread one hot item across **all Redis shards**, so:
  - no single shard carries the whole drop
  - if one shard fails, only part of the stock pauses, not all of it
