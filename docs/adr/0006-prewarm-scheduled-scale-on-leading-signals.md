# ADR-0006: Start servers before planned drops; buy time for surprise spikes

## Context

Traffic jumps from 2K to 500K requests/s in 30 s. A new server takes 2–3 min to be ready: get a machine (1–2 min), download our app (~20 s), start and pass health checks (~15 s). That's 4–6 times slower than the spike (02-scale-and-numbers.md, point 10).

The old system added servers when CPU went up. But CPU stayed low, because the servers were waiting on database locks and connections, not working. Some spikes are planned (drops); others come without warning (viral posts, bots).

## Decision

**Planned drops: start servers early (pre-warm).** 30 min before the drop:
- start the servers needed for the drop (24 gateway, 24 Hold, 12 checkout, 8 payment worker, 6 order worker; 06-scalability-and-fault-tolerance.md, section 1)
- set the autoscaling minimum to that count, so they aren't removed while traffic is still low
- open database connections ahead of time, and ask the cloud load balancers to get ready for high traffic
- load the stock into Redis, and put the drop pages on the CDN

No deploys from 24 h before the drop until 30 min after sell-out.

**Surprise spikes: buy time, then add servers.**
1. **The waiting room switches on by itself within 10 s** when traffic reaching us passes 10K requests/s (5× a quiet day) or Hold p99 passes 400 ms (before our 500 ms target is missed). It then lets people in only as fast as the current servers can handle.
2. **Autoscaling looks at requests/s and requests in progress per server**, and for workers, the Kafka backlog. CPU is only a safety limit.
3. **~25% spare room on the machines**, kept by low-priority placeholder servers. A new real server takes their place at once, so it doesn't wait 1–2 min for a machine.
4. **The admission controller lets more people in** only as new servers become ready.

## What we didn't pick

| Option | Why not |
|---|---|
| Add servers on CPU only | CPU stays low while servers wait on the database, and it rises only after the damage is done (06-scalability-and-fault-tolerance.md, section 1). This is what broke the last drop |
| Keep all drop servers on all month | ~75 servers × 730 h = ~55,000 server-hours a month, for a sale that lasts ~15 min. Starting early costs ~75 server-hours per drop (~75 servers × ~1 h) |
| Turn servers off completely between drops | every drop would start from cold, which is the problem we're solving |
| Start servers early only | covers planned drops only; a surprise spike would hit a cold system |

## Consequences

- **Good:**
  - planned drops start with every server, connection and cache ready
  - surprise spikes see a queue, not errors
  - autoscaling reacts as soon as requests rise, before users feel it
- **Bad: servers sit idle for ~1 h per drop, plus ~25% spare room always on.** **Fallback:** we accept it. A failed drop costs far more in refunds, support and reputation.
- **Bad: the start-early steps must be done for every drop.** **Fallback:** they run by themselves from the drop schedule, and on-call checks a short list 15 min before the start.

## When to revisit

- New servers start much faster than the spike (for example from saved snapshots).
- Drops happen so often that always running more servers is cheaper than starting early each time.
