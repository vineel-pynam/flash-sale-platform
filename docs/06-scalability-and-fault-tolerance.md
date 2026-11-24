# Scalability & Fault Tolerance

## 1. Scaling our servers

- **Servers are stateless:** no user data lives in a server. Who you are is in the entry pass, stock is in Redis, money is in Postgres. So any server can answer any request, and losing one only loses its in-flight requests, which are safe to retry.

| Service | Scales on | Per server | Ready before the drop | Max |
|---|---|---|---|---|
| API Gateway | requests/s + requests in progress | 2,000/s | 24 | 48 |
| Hold service | requests/s + requests in progress | 2,000/s | 24 | 48 |
| Checkout service | requests in progress | 50 at once | 12 | 24 |
| Payment worker | Kafka backlog | – | 8 | 16 |
| Order worker | Kafka backlog | – | 6 | 16 |
| Outbox sender, fix-up job | fixed | – | 3, 2 | 3, 2 |

- **Why 24:** ~15K req/s reach our servers (new people's Holds + people in checkout; doc 02, point 6). We build for 30K/s (2×) ÷ 2,000 per server = 15 servers. They must still fit in 2 availability zones if one fails → 8 per availability zone × 3 = **24**.
- **Max 48:** 2× more if traffic is higher than planned. Gateway and Hold servers use no database connections, so more of them doesn't hurt Postgres.

**Why not scale on CPU:**
1. **Wrong signal.** Last time, servers were stuck waiting on the database with low CPU.
2. **Too slow.** CPU is averaged over minutes; the spike arrives in 30 s.
3. **Too late.** By the time CPU rises, requests are already queuing.

**Ready capacity:**
- **Pre-warm** before planned drops.
- **~25% spare room** on the machines, kept by low-priority placeholder servers that real ones replace instantly.
- **Fast start:** small Go program; the app image is already on every machine (pre-pull DaemonSet, see 05, section 2.4).

## 2. Avoiding one hot spot

A **hot spot** is when all the work lands on one place (one key, one row, one partition) while the rest sit idle. The last drop failed this way: every buyer had to lock the same stock row.

**Rule:** to spread the work, split by a value that is **different for each request**, like the user ID or the order ID. Don't split by the drop ID: during a sale everyone has the **same** drop ID (`drop-1`), so every request would go to the same place.

| Store | What is split | Split by | How a request finds its place | Example |
|---|---|---|---|---|
| **Redis** | the 50,000 units, into 50 lists (buckets) of ~1,000; the 50 buckets sit on 6 shards | **unit number**: unit 1 → bucket 0, unit 2 → bucket 1, ... unit 51 → bucket 0 again | a Hold goes to the user's **home bucket** = `hash(user ID) % 50`, so 10M users spread over all 50 buckets | `user-123` → bucket 12 → shard 2 |
| **Kafka** | payment jobs, into 48 parts (partitions) | **order ID** | part = `hash(order ID) % 48`; every job for the same order lands in the same part, in order | order `a1b2...` → part 17 |
| **Postgres** | **not split** | – | – | – |

**Why Postgres needs no split:**
- **Before:** every buyer **changed** the same stock row, so they waited in line for its lock.
- **Now:** each buyer **adds** their own new rows (order, unit, payment, job), so nobody waits for anybody.
- ~3,000 new rows/s is easy for one primary.

**Two more points:**
- **More drops at once:** one Postgres primary is enough for one drop. With many big drops at the same time, each drop gets its own database (v2). We split by drop here because each drop's unique rules ("one unit sold once") must be checked in one place.
- **Reads:** "paid yet?" checks and order history are read from Postgres copies, so the primary only handles saves.

## 3. Overload: what to reject first

**Each layer slows the one before it, so extra work waits in the cheapest place:** PSP slow → payment workers slow down → jobs wait in Kafka → admission controller lowers the let-in rate → people wait in the waiting room (free, and they see their place).

- **Example:** the PSP slows from 0.4 s to 2 s → charges in progress hit ~1,600 → new jobs wait in Kafka → the let-in rate drops (for example to 500/s) → people wait in the waiting room, where they see their place.

**Every part has a limit:** the most work it takes on at the same moment. Without a limit, requests pile up, everyone gets slow, retries add even more load, and the system collapses (the last drop: 25 s). With a limit, a part only takes what it can finish quickly; the rest get "try again in X s" right away, and the retry a moment later usually works.

| Limit | Value | Why this number | When reached |
|---|---|---|---|
| Requests one gateway server works on at the same moment | 200; lowered by itself when responses get slower | 2,000 requests/s × 50 ms each ÷ 1,000 = 100 at the same moment; ×2 for room | "try again in X s" at once; lowest priority first |
| Card charges waiting on the PSP at the same moment | ~1,600 | 800 charges/s × up to 2 s each | new payment jobs wait in Kafka |
| Longest wait for a free Postgres connection | 100 ms | a save normally takes a few ms | give up, answer "try again" |
| Longest wait for a free Redis connection | 50 ms | Redis normally answers in < 1 ms | give up, answer "try again" |

- **"At the same moment"** is not "per second": a request counts from when it arrives until it's answered. With 2,000 arriving per second and each taking 50 ms, about 100 are in progress at any moment.
- **"Lowered by itself":** if responses slow down (for example to 100 ms), the same 200 slots fill with slower requests, so the server lowers its limit and doesn't take on more than it can finish.

**Who gets rejected first:**

| Priority | Traffic | When busy |
|---|---|---|
| lowest | suspected bots | dropped first |
| low | anonymous browsing | old cached page |
| medium | logged-in browsing, order history | cached, or rejected when very busy |
| high | Hold (has an entry pass) | protected: we lower the let-in rate instead |
| highest | Pay, status, PSP webhook | rejected last; if at risk, pause admissions first. A rejected Pay gets "try again in 2 s" and the browser retries with the same key |

**Slowing down step by step.** **Busy** = requests in progress ÷ the server's limit (200), averaged over all gateway servers. For example, 140 in progress = 70% busy.

| Level | When | What we turn off | What still works |
|---|---|---|---|
| Normal | under 70% busy | nothing | everything |
| Protect | over 70% busy | extras: recommendations, reviews | product pages, Hold, Pay |
| Checkout only | over 85% busy | live pages: all pages except checkout are served as ready-made copies from the CDN; 30% fewer people let in | Hold, Pay, "paid yet?" |
| Pause | Redis, Postgres or the PSP is down | letting new people in; the queue shows "paused, your place is kept" | payments already started finish; holds don't expire, so people holding a unit can pay once it's back |

The gateway switches levels by itself from the "busy" number; on-call can also switch by hand.

## 4. Retries, timeouts, circuit breakers

### Retries

**Rules:**
- **Retry in one place only:** the user's browser for Hold and Pay, the payment worker for the PSP. Services don't retry each other. If every layer retried 3 times, one click could become 3 × 3 × 3 = 27 calls at the bottom, right when that part is struggling.
- **Growing waits with randomness:** each retry waits longer than the last, plus a random amount. If 10,000 people all retried after exactly 2 s, they'd come back as a new spike; random waits spread them out.
- **Every "try again" says when** (`Retry-After`): 1 s for a Hold already in progress, 2 s for Pay, 2–6 s (random) when a server is overloaded.
- **Every retry uses the same key**, so it can never create a second hold or a second charge.

| Who retries | What fails | Tries | Wait between tries | After the last try |
|---|---|---|---|---|
| The user's browser | a Hold or Pay request gets "try again" or times out | 3, same idempotency key | what the server said ("try again in X s") | the user sees an error and can click again |
| Payment worker (quick tries) | one PSP call times out or errors | 5, same key; each call can take up to 2.5 s | random, up to 0.25 s, 0.5 s, 1 s, 2 s | the job moves to the retry topics |
| Retry topics | a payment job that failed its quick tries | 3 more rounds | 5 s, then 30 s, then 2 min | the fix-up job asks the PSP what happened |

### Timeouts shrink at each step

A timeout is the longest we wait for an answer. Each inner call gets **less** time than the request around it:

| User's request | Its timeout | Inner call | Its timeout | Why shorter |
|---|---|---|---|---|
| Hold | 2 s | Redis | 100 ms | Redis normally answers in < 1 ms; if it takes 100 ms, something is wrong, so fail fast while there's still time to answer the user |
| Pay | 3 s | Postgres | 500 ms | a save normally takes a few ms; 500 ms leaves time to answer "try again" before the user's 3 s are up |
| – (background) | – | one PSP call | 2.5 s | just above the PSP's slow case (2 s), so normal slow calls succeed and stuck ones are cut |

- **Why:** once the user's request has timed out, nobody is waiting for the answer any more. If the inner call were allowed longer, our server would keep working on an answer nobody will read, and hold resources other users need.
- **Not too strict:** each timeout is ~100× the normal time (Redis < 1 ms vs 100 ms; a save takes a few ms vs 500 ms), so they only trigger when something is already broken.
- **Pay's 3 s covers only saving the order.** The charge happens later in the background, with no user request waiting. If the browser times out after the order was saved, its retry (same key) gets the saved answer, so there's no second order.

### Circuit breakers

When a dependency keeps failing, **stop calling it for a short time** instead of hammering it:
- While the breaker is open, calls fail **at once**, with no waiting for timeouts.
- After a pause, a few test calls check whether the dependency is back.

| Breaker | Protects | Opens when | While open | Closes when |
|---|---|---|---|---|
| Payment worker → PSP | the PSP, and our payment workers | in 10 s, at least 20 calls and half of them failed | no PSP calls; jobs wait in Kafka; let-in rate lowered | after 10 s, 5 test calls all succeed |
| Checkout → Postgres | Postgres | half the calls fail | Pay answers "try again"; holds don't expire | test calls succeed |
| Hold → Redis | Redis | 30% of calls fail | Holds fail fast; admissions paused | test calls succeed |

### Keep slow parts apart (bulkheads)

Different kinds of work get their own servers and their own database connections, so one slow part can't block the rest.

- **PSP calls run only in the payment workers.** If the PSP gets slow, only the payment workers fill up, and jobs wait in Kafka. The servers that answer users never call the PSP, so Hold, Pay and "paid yet?" stay fast.
- **Separate database connections for users and for background work.** Postgres allows only a limited number of connections (our budget is 290 of 400, doc 02 point 9). The servers that answer users (Checkout, status) get their own share, and the background workers (payment worker, outbox sender, fix-up job) get a separate share. If the workers get busy, for example catching up on a payment backlog, they can only use up **their** share; a user's Pay always finds a free connection.

## 5. Safe to repeat (idempotency)

With retries everywhere, the same request or message **will** arrive twice. **Safe to repeat** means doing it twice has the same result as doing it once: the second time we notice it and return the first result instead of doing the work again.

**Two layers of protection:**
1. **Same key:** catches a retry of the **same click**. The browser makes the key once per button and sends it again on every retry.
2. **Business rules:** catch a **new click** for something already done (one hold per person, one order per hold). A refresh makes a new key, so this layer catches it.

**Requests from the user:**

| Repeat | Example | What catches it | Result |
|---|---|---|---|
| the same Hold or Pay click arrives twice | double click, browser retry after a timeout | `Idempotency-Key`: Redis saves the answer (30 s while the request is running, then 24 h, for apps that resend the same request later) | the first answer is returned again |
| a new Hold by someone who already holds one | the user refreshes and clicks Hold again | the user key in Redis ("user-123 holds 41-abc") | `200`: their existing hold |
| a new Pay for a hold already being paid | the user clicks Pay again on a fresh page | Postgres: one order per hold (`reservation_id` unique) | `409`: already bought |
| two people buying the same unit | rare Redis failover gives one unit to 2 people | Postgres: one sale per unit (`event_id, unit_id`) | the second gets `409`, before any charge |

**Background work:**

| Repeat | Example | What catches it | Result |
|---|---|---|---|
| a charge is sent twice | worker retry, or a duplicate payment job | the PSP, keyed by `reservation_id` | the PSP returns the first charge; no new money taken |
| the PSP's result arrives twice | the worker's call **and** the webhook | Postgres: update only if the payment is still PENDING | the second copy changes nothing |
| a refund is sent twice | the fix-up job resends | the PSP + Postgres, keyed by `refund_id` | the PSP returns the first refund |
| a Kafka message is delivered again | a worker crashed before saying "done" | `processed_messages` (message ID) | skipped |

## 6. Partial failures

**Rule: when we're not sure, don't guess.** Never mark a payment failed and never give a unit away until we know. Keep it as it is, retry with the same key, and later ask the source of truth (the PSP or Postgres). Every half-done step is either finished or undone by the fix-up job.

All cases (no answer from the PSP, worker crash, checkout crash, Redis or Postgres failover, Kafka down) are in `04-architecture.md`, section 3.2.

## 7. Availability zones, regions, RPO/RTO

**Two terms:**
- **RPO (Recovery Point Objective):** how much data we can lose in a failure, counted in time. "RPO 0" = nothing lost.
- **RTO (Recovery Time Objective):** how long until we're working again.

**Where things run:**
- **Availability zone:** a separate data centre with its own power and network. Our home region has 3 of them, a few ms apart, so copying between them is fast.
- **Region:** a group of availability zones in one part of the world (for example `us-east`). Each drop has one **home region** that owns its stock. A second, **standby region** far away is ready to take over.

### When one availability zone fails

This can happen (a power or network failure in one data centre). We lose a third of our machines, but every part has copies in the other two availability zones:

| Part | How it's spread | What happens when one availability zone fails | Data lost |
|---|---|---|---|
| Our services | servers in all 3 availability zones | the other 2 availability zones take the load; 16 of 24 servers left, still enough (section 1) | none |
| Redis | each shard has a copy in another availability zone | the copy takes over in < 15 s. Meanwhile only Holds on that shard pause; the other shards keep selling | maybe ~1 s of holds (Postgres still stops any double sell) |
| Postgres | primary + 2 copies; each save waits for at least 1 copy | a copy becomes the primary in ~30 s. Meanwhile Pay answers "try again"; holds don't expire | none |
| Kafka | 3 copies of every message, one per availability zone | the other copies keep serving | none |

**Result:** a short blip (< 1 min), no money or orders lost.

### When the whole home region fails

This is very rare (all 3 availability zones down at once, for example a cloud-wide outage).

**What the standby region has ready:**
- a copy of Postgres, kept up to date all the time
- servers ready to start
- Redis is not copied; it's rebuilt from Postgres

**Why the standby copy can be a few seconds behind:** saves don't wait for it. Waiting for a copy that far away would add ~70–150 ms to every save. So it trails by up to ~5 s, and some orders from those last seconds, even paid ones, may be missing in the standby region.

**How we still don't lose money:** the PSP keeps its own record of every charge, outside our region. Every charge carries our `reservation_id` as its key, plus `order_id`, `user_id`, `drop_id` and `unit_id` as metadata, so we can rebuild any missing order from the PSP.

**Switching over (manual, < 15 min):**
1. pause admissions (people keep their place in the queue)
2. make the standby Postgres the new primary
3. **recover the missing seconds from the PSP:** the fix-up job lists the PSP's charges from the last 15 min. A charge with no order in Postgres → rebuild the order from the metadata: PAID if the unit is still free, otherwise refund.
4. rebuild Redis from Postgres (now complete)
5. point the CDN at the new region, resume. Charges that were still running finish by PSP webhook, which the PSP keeps retrying until the new region answers.

The daily check against the PSP's settlement report (doc 07) catches anything still missed.

**Why manual:** an automatic switch can fire by mistake (a network blip can look like a region failure). A wrong switch in the middle of a drop is worse than a short pause, so a person confirms it first.

**Result:** up to 15 min paused. No money lost; a few unpaid holds from the last seconds are lost, and those users hold again.

### Summary

| Failure | Data lost (RPO) | Back in (RTO) |
|---|---|---|
| One server | none | seconds |
| One availability zone | none (Redis may lose ~1 s of holds; Postgres stops any double sell) | < 1 min |
| Home region | up to ~5 s of saves; no money lost (orders rebuilt from the PSP's records); a few unpaid holds lost | < 15 min |
| PSP outage | none (payments wait in Kafka) | depends on the PSP |

## 8. Keeping 99.95%

99.95% = **22 min of errors allowed per month** (43,200 min × 0.05%), or **500 failed Holds in a drop of 1M** (doc 07). Two things use it up: **our deploys** and **other systems' outages**.

**Deploys:**
- **No deploys** from 24 h before a drop until 30 min after sell-out.
- **Rolling updates:** add new servers before removing old ones, so capacity never drops.
- **Graceful shutdown:** stop taking new requests, finish the current ones, then exit.
- **Canary:** 1% → 10% → 50% → 100%, with automatic rollback if errors go up.
- **Database changes in 3 steps** (add new → move data → remove old), so old and new code both work.
- **Feature flags** to switch risky features off without a deploy.

**Other systems down:** when one part breaks, we pause or delay, keep everyone's place and unit, and never lose money or orders.

| Down | Normally it | What users see | What we do |
|---|---|---|---|
| Waiting room vendor | holds the queue, lets ~1,100/s in | "high demand, please wait" | gateway lets in only 30K req/s (our design capacity); no fair queue order while it's down |
| Redis | gives out units for every Hold and Pay | queue paused, place kept | stop letting people in. Payments already started finish (marked sold when Redis is back); holders keep their unit and can pay once it's back |
| Postgres | saves every order | "payment temporarily unavailable" | Pay answers "try again" at once; holds don't expire; a copy takes over in ~30 s |
| Kafka | carries payment jobs to the workers | "processing payment" takes longer | jobs are safe in the outbox table and are sent once Kafka is back |
| PSP | charges cards | "payment delayed, your unit is held" | breaker stops calls; jobs wait in Kafka; fewer people let in, then paused |
| Email | sends confirmations | the email comes later | orders are still confirmed in the app; emails retried |

## In the prototype

- **Built:**
  - separate limits for Hold and Pay requests in progress, with an instant "try again in 2–6 s" (`503`) when full
  - deadlines on every request, passed down to Redis, Postgres and the PSP
  - Redis: 100 ms per call, 50 ms max wait for a connection; Postgres: 300 ms per statement (inside Pay's 500 ms budget)
  - PSP: 5 tries with random growing waits, and a circuit breaker
  - idempotency keys (saved answer 24 h) and safe retries with the same key
  - separate database connections for the API and the background workers
  - graceful shutdown
  - rebuilding Redis from Postgres
- **Design only:**
  - autoscaling
  - priority classes and slow-down levels
  - limits that adjust themselves (the prototype's are fixed)
  - Kafka with retry topics and a dead-letter topic (the prototype's workers read the outbox directly)
  - the admission controller
  - Postgres and Redis circuit breakers
  - charge metadata sent to the PSP
  - multiple availability zones and regions
