# Trade-offs

## 1. Our design choices

| Choice | We picked | Instead of | Why | What we give up |
|---|---|---|---|---|
| **Where stock lives** | Redis answers every Hold (reservation) click: "you got a unit" or "sold out". Postgres keeps the final record of who bought what | Postgres only, or Redis only | Postgres alone is too slow for the rush (last drop: 25 s). Redis alone can lose the last ~1 s of sales if a node dies | the two can disagree, so the fix-up job repairs Redis from Postgres |
| **How stock is stored** | a list of unit IDs (U000001, U000002, ...) | one counter (stock = 50,000) | each buyer gets a real unit, and if they don't pay, that exact unit goes back on the list | a bit more memory (a few MB) |
| **Splitting stock in Redis** | 50 small lists (buckets) of ~1,000 units each, spread over the Redis shards | one list holding all 50,000 units, on one shard | with one list, one shard does all the work and is a single point of failure. 50 lists share the work, and if a shard fails only part of the stock pauses | when a buyer's bucket is empty, the Hold tries another one |
| **When a hold runs out** (a buyer has 3 min to pay) | a list of holds sorted by end time; the hold expiry job puts each expired unit back | let Redis delete the hold by itself (TTL) | when Redis deletes the hold, the unit ID inside it is deleted too, so that unit is never sold. The job returns it, and skips holds that are being paid | one more job to run and watch |
| **What happens first when the user pays** | save the order in Postgres first, then charge the card | charge the card first | with charge first, a crash can leave a user who paid but has no order, and a unit sold twice means taking money and refunding it. Saving first means Postgres rejects a sold unit before any charge, and a crash leaves a "payment pending" order that the fix-up job finishes | the user sees "processing payment" for 1–3 s |
| **Charging the card: now or in the background** | reply "payment started" at once; a worker charges the card in the background | keep the user's request open until the PSP answers | the PSP is slow (up to 2 s, sometimes timeouts). Waiting requests fill our servers and slow everything, as in the last drop. In the background, workers call the PSP at a safe rate (≤ 1,000/s) | the user sees "processing payment" for 1–3 s |
| **Sending jobs to Kafka** | save the job in Postgres (outbox table) together with the order; the outbox sender sends it to Kafka later | save the order in Postgres, then send the job to Kafka | if the server crashes after saving the order but before sending to Kafka, the job is lost and the card is never charged. With the outbox, the job is saved with the order, so it can't be lost, and the sender keeps trying until Kafka has it | jobs reach Kafka ~50 ms later |
| **How the outbox sender finds new jobs** | polling: every 50 ms it asks Postgres for rows not sent yet | CDC: a tool (for example Debezium) reads Postgres's change log (WAL) and sends new rows | polling is a small loop in our own code, easy to run and debug, and fast enough for our ≤ 1,000 jobs/s. CDC needs Postgres setup (a restart, a replication slot) and one more system to run and watch | a small extra load on Postgres, and up to 50 ms delay; we move to CDC if jobs grow much more |
| **Duplicate jobs** | Kafka may deliver a job twice ("at least once"); each worker skips a job it has already done | Kafka's "exactly once" mode | "exactly once" only covers Kafka itself, not the PSP call, Postgres or emails, so we need duplicate checks anyway. The payment worker uses the reservation ID as the PSP key, so a repeat can't charge twice | each worker saves which jobs it has done (`processed_messages` table) |
| **Retrying a failed payment job** | a few quick tries (same key); still failing → move it to a retry topic (5 s, 30 s, 2 min) → then the fix-up job. If the PSP is down, workers pause and jobs wait in Kafka. A job that can never work (bad data) goes to a dead-letter topic | keep retrying the same job in the worker until it works | Kafka gives jobs in order, so while one job waits, every job behind it waits too. Moving it aside lets the others go on | 3 more topics to run, plus a dead-letter topic; a retried job may finish after newer ones |
| **Which region sells a drop** | one region sells all the stock of a drop (with 3 availability zones inside it); other regions only show pages | each region sells its own share, or all regions sell from the same stock | if each region has its own share, one region can sell out while another still has units left. If all regions share the stock, they must agree on every Hold, which is slow and can sell the same unit twice | if that whole region fails, the drop pauses for up to 15 min; far-away users wait ~100–200 ms longer |
| **When a save counts as done** | Postgres keeps 2 copies in other availability zones, and says "saved" only after at least 1 copy also has the data | say "saved" at once and send the data to the copies a moment later | if the main Postgres dies in that moment, the copies never got the last saves, so a paid order could be lost. Waiting for 1 copy means nothing is lost. Waiting for only 1 (not both) means one slow copy doesn't slow every save | each save takes ~1–2 ms longer |
| **How fast people are let in** | only as fast as the PSP can charge them (~1,100/s). Our admission controller works it out every 5 s (PSP health, payment backlog, free stock) and sends it to the waiting room; 0 when no stock is free | as fast as our servers can handle (~15,000/s) | the PSP charges only 1,000 cards/s. If we let in more, people hold units but wait too long to pay, their holds expire, and the units go unsold | a longer queue than our servers alone would need |
| **How payment workers keep up with a slow PSP** | each worker charges many orders at the same time (~35 per Kafka partition × 48 partitions ≈ 1,680) | one charge at a time per partition, and add more partitions to go faster | when the PSP takes 2 s per charge, ~1,600 charges are waiting at once. One at a time would need ~1,600 partitions, which is too heavy for Kafka. Each order has its own job, so charging them side by side is safe | the worker must track which jobs are finished before telling Kafka "done up to here" |
| **Holding a unit when your home bucket is empty** | 3 small steps: mark yourself "pending" in your home bucket (5 s), take a unit from another bucket, then save the hold ID in your home bucket | lock both buckets together (they are on different Redis shards) | a lock across shards is slow and complex, and this case is rare (only near the end of the sale). The "pending" mark stops a second search by the same person, and Postgres blocks a second order anyway | if a step fails, a unit can be stuck until its hold expires (3 min), or the user is told "try again in 1 s" (up to 5 s if our server crashed mid-search) |
| **Database for orders and payments** | Postgres | a NoSQL store (for example DynamoDB) | paying saves 4 rows that must all be saved or none (order, unit, payment, outbox job), and our unique rules ("one unit sold once", "one unit per person") are one line each in Postgres. In NoSQL both are harder and easy to get wrong. ~3,000 rows/s is easy for Postgres | if writes ever grow ~100×, one Postgres primary becomes the limit and we must split the data |

## 2. Trade-offs from the brief

### 2.1 Fairness vs throughput
| | Max throughput | Fairness (picked) |
|---|---|---|
| **How people get in** | everyone clicks Hold at once | the waiting room lets in ~1,100 people/s |
| **Who gets a unit** | the fastest connections and bots | queue order (see below) |
| **What happens to the system** | a huge spike that breaks it, as in the last drop (25 s p99) | steady load; every Hold is answered fast |
| **How long the sale takes** | seconds, if the system survives | ~10–12 min. All 50,000 units are reserved within ~50 s, then units from declined cards and expired holds go to the next people in the queue |

**Queue order:**

| When you join | Your place |
|---|---|
| before the sale starts, or within 30 s after it | one random place among everyone in this group |
| after that | the end of the queue, first come, first served |

- **Why the shuffle:** most of the 10M people arrive in the first 30 s, so an arrival order would be decided by milliseconds. The shuffle means joining at the exact second doesn't help. Bots are checked when they join, and any that get through are shuffled like everyone else.
- **Example:** Alice joins at 9:40, Bob at 9:59 and Carol at 10:00:20, so all three are shuffled together. Dave joins at 10:02 and goes after all of them.
- **We give up:** a slower sale. Also, someone who joined 20 min early may end up behind someone who joined 10 s after the start. The queue page says this up front.

### 2.2 Strong consistency vs availability
| Part | We choose | What it means |
|---|---|---|
| **Selling a unit, payments** (Postgres) | consistency | a unit is sold once, a paid order is never lost. If Postgres is down, Pay answers "try again" and holds don't expire |
| **Holding a unit** (Redis) | speed, with a safety net | after a rare Redis failover, a unit may be held by 2 people. Postgres lets only one of them buy it, before any charge |
| **Pages, "how many left", sold out** (CDN) | availability | always served, even when our services are down; may be 1–2 s old |

- **Why:** a short pause is a small wait. Selling a seat twice, or losing a paid order, means an angry customer whose money we took.
- **We give up:**
  - if one Redis shard is down, part of the stock pauses
  - if the whole home region is down, the drop pauses for up to 15 min

### 2.3 Instant confirmation vs background processing
| Step | Instant or background | Why |
|---|---|---|
| **Hold** | instant: the real answer in a few ms | uses only Redis, so the user knows right away if they got a unit |
| **Pay accepted** | instant: "payment started" | only saves the order in Postgres (a few ms) |
| **Card charged** | background: result in 1–3 s | the PSP is slow (up to 2 s, sometimes timeouts). Waiting for it inside the request filled our servers last time |
| **Order confirmed + email** | background: within 30 s | not needed in the same second |

- **Rule:** answer instantly from our own fast stores (Redis, Postgres); use the background for slow outside systems (the PSP, email).
- **We give up:** a 1–3 s "processing payment" screen, and more moving parts (outbox, Kafka, workers, fix-up job).

### 2.4 Spare servers vs autoscaling delay (cost vs risk)

**Problem:** traffic jumps 250× in 30 s, but a new server takes 2–3 min to be ready. If we wait for traffic before adding servers, they arrive too late.

**What we do:**

| Case | What we do | Cost |
|---|---|---|
| **Planned drop** (we know the time) | start all 24 servers ~30 min before the drop, stop them after it ends | ~1 h of 24 servers per drop |
| **Surprise spike** (no warning) | the waiting room lets people in only as fast as our servers can handle; spare room lets new servers start in seconds, not minutes | ~25% extra machine space, always on. People wait in the queue 2–3 min; nothing fails |

**What we didn't pick:**

| Option | Why not |
|---|---|
| 24 servers always on | safe, but very expensive, because the servers sit idle most of the month |
| Add servers only when traffic rises (autoscaling only) | cheapest, but the servers arrive 2–3 min late and the drop fails |

**How spare room works:**
- **Placeholder pods:** low-priority pods that do nothing and keep ~25% of machine space booked. A new real server takes their place instantly.
- **Pre-pull DaemonSet:** one small pod on every machine downloads our app image ahead of time, so a new server doesn't wait to download it.

**Why we pay for this:** one failed drop (refunds, support, reputation) costs more than a year of pre-warming.

### 2.5 Build vs buy

- **Buy** = pay a company for a ready-made service that they run for us (we only configure it).
- **Build** = our own team writes the code and runs it.
- **Rule:** buy what isn't our product and is hard to run at this scale; build what holds our business rules.

| Part | Decision | Why |
|---|---|---|
| Waiting room | **buy** | must take ~333K checks/s from day one, and keep working even when our services are down |
| Bot protection | **buy**, add our own signals later | vendors see bots across thousands of sites |
| Kafka | **buy** a managed service | running Kafka across 3 availability zones is a full-time job |
| Redis, Postgres | **buy** managed services | the provider handles failover, patching and backups. Managed Postgres can wait for 1 of 2 copies, as we need |
| CDN, Kubernetes | **buy** managed services | standard parts, not our product |
| Admission controller | **build** | it holds our rule: let people in only as fast as the PSP and the stock allow |
| Hold, Checkout, workers, fix-up job | **build** | this is where "never sell twice, never charge twice" lives, so we must own and test it |
