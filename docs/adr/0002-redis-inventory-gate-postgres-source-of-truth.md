# ADR-0002: Redis decides holds, Postgres keeps the truth

## Context

A **Hold** (reservation) is the first click: the buyer gets one specific unit kept for them for 3 min while they pay. If they don't pay in time, the unit goes back for someone else.

At the last drop, every buyer updated the same stock row in Postgres. One row can take only ~200–500 updates a second, one after another, but ~10K/s wanted in. Postgres slowed down, and the same seat was sold twice.

A Hold must answer in under 500 ms (p99), and only ~15 ms of that is for picking a unit (02-scale-and-numbers.md, point 11). We need something fast enough for every click, and something strict enough that money is never taken for a unit sold twice.

## Decision

1. **Redis decides each Hold.** The stock is stored as **lists of unit IDs**: 50 lists (buckets) of 1,000 units, spread over 6 Redis shards. One Lua script (a small program that runs inside Redis, with nothing else running in between) checks "this person has no hold yet", takes a unit, and saves the hold. If your home bucket is empty, the Hold takes a unit from another bucket in 3 small steps (05-trade-offs.md, row 15).
2. **Postgres keeps the final record.** Two unique rules on the `allocations` table: one unit is sold once (`PRIMARY KEY (event_id, unit_id)`), and one unit per person (`UNIQUE (event_id, user_id)`). This row is saved before the card is charged (ADR-0008).
3. **Expired holds** (3 min) are handled by the hold expiry job. It puts the unit back on the list. We don't let Redis delete the hold by itself, because the unit ID inside it would be lost too.
4. **After a normal Redis failover,** the copy takes over and the fix-up job repairs any gap. If Redis loses its data, buckets are **rebuilt from Postgres** before people are let in again.

## What we didn't pick

| Option | Why not |
|---|---|
| Postgres, one stock row (`qty = qty - 1`) | this is the row that broke the last drop |
| Postgres, one row per unit with `SKIP LOCKED` | no waiting on locks, but every Hold click still hits the database. We use it only in v0, behind a lower let-in rate (08-execution-plan.md, section 2) |
| Redis counter (stock = 50,000) | fast, but after a failover nobody can tell which sale was extra, and Postgres has no unit to check |
| Redis only, no Postgres rules | Redis copies data to its backup a moment later. After a failover it can sell a unit twice, with nothing to stop it |
| One Redis list for all stock | one shard does all the work, and if it fails the whole drop stops (02-scale-and-numbers.md, point 12) |
| A global SQL database (Spanner, CockroachDB) | strict, but 10–50 ms per save and much more expensive. Redis + Postgres gives the same safety for less |

## Consequences

- **Good:**
  - a Hold is decided in 5–15 ms
  - Postgres only sees payments (≤ 800/s), not every click
  - a unit given out twice is rejected by Postgres before anyone is charged
- **Bad: two stores that must agree.** **Fallback:** the fix-up job compares them every 1 min and repairs Redis. Full rebuild from Postgres if Redis loses its data.
- **Bad: after a rare Redis failover,** a user may be told "you got a unit", then rejected at Pay. This needs a failover inside a ~1 s window, and it costs the user one retry, never money.
- **Bad:** each Lua script can only touch keys on one Redis shard, which shapes how we name keys (03-entities-apis-schema.md, section 3).

## When to revisit

- A drop has more than ~1M units (the lists get big; switch to counters and pick the unit in Postgres).
- Redis can copy each save to its backup before answering, without getting slow.
- Saves to `allocations` get close to what one Postgres primary can handle, across drops at the same time.
