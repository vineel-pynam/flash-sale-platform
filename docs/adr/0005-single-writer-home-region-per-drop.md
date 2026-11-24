# ADR-0005: One home region sells each drop

## Context

A **Hold** (reservation) is the first click: the buyer gets one specific unit kept for them for 3 min while they pay. If they don't pay in time, the unit goes back for someone else.

Buyers are in many regions. A unit must never be sold twice, and a Hold must answer in under 500 ms (p99). All 50,000 units are held within ~50 s of the start (02-scale-and-numbers.md, point 7), so there's no time to move stock between regions during a drop.

## Decision

1. **Each drop has one home region**, chosen near most of its expected buyers. All of the drop's stock lives only there: Redis buckets, Postgres orders, Kafka topics.
2. **Other regions** run the CDN, the waiting room and the pages. Every Hold and Pay, from anywhere in the world, is sent to the home region. The CDN keeps its connections to the home region open, so it doesn't have to connect again for each request. The worst case is 180 ms of travel (02-scale-and-numbers.md, point 11).
3. **A standby region** gets a copy of Postgres. Saves don't wait for that copy, so it can be up to ~5 s behind. If the whole home region fails, a person decides to switch (< 15 min):
   1. pause admissions
   2. make the standby Postgres the new primary
   3. rebuild missing orders from the PSP's records: every charge carries our order, user, drop and unit IDs
   4. rebuild Redis from Postgres
   5. resume

   Full steps: 06-scalability-and-fault-tolerance.md, section 7.

## What we didn't pick

| Option | Why not |
|---|---|
| **Each region gets its own share of stock** (for example 20K Americas, 20K Europe, 10K Asia) | demand never matches the split: one region sells out while units sit unsold in another. Moving units between regions during the first ~50 s needs all regions to agree, which is slow and complex. People in a smaller region would also get a better chance |
| A database that writes in many regions at once (Spanner, CockroachDB, DynamoDB global tables) | the strict ones need a round trip to another region (100 ms+) on every Hold. The faster ones let two regions sell the same unit at the same moment |
| Switch to the standby region automatically | a network blip can look like a region failure. A wrong switch in the middle of a drop can let both regions sell at once, which is worse than a short pause |

## Consequences

- **Good:**
  - one region decides every Hold, so no region has to check with another
  - one unit is sold once, and that's easy to check
  - every buyer competes for the same stock, so no units are left unsold in another region
- **Bad: far-away users wait up to ~180 ms longer.** **Fallback:** it still fits within 500 ms, with 260 ms to spare.
- **Bad: if the home region fails, the drop pauses for up to 15 min.** **Fallback:** this is very rare. Meanwhile the queue and the pages keep working on the CDN, and no money is lost.

## When to revisit

- Drops where stock really is per region (for example stock in separate warehouses per region). Then a share per region makes sense.
- Users far away keep complaining about speed, and the CDN can't fix it.
- Many drops run at the same time. Then we give them different home regions, to spread the load.
