# ADR-0001: Buy the waiting room and bot protection; random order at the start

## Context

Up to 10M people wait for a drop. Their browsers checking "is it my turn?" alone make ~333K requests/s (02-scale-and-numbers.md, point 3). That traffic must never reach our servers. The waiting room must also keep working when our home region is down, so it has to run on the CDN, not on our servers. Only 0.5% of people can get a unit, so bots are the main threat to fairness.

The next drop is weeks away, so whatever we choose must be live before then.

## Decision

1. **Buy** a waiting room that runs on the CDN (Cloudflare Waiting Room, Queue-it or Akamai, chosen on price and how well its API fits) for v0 and v1.
2. **Buy** bot protection that runs on the CDN. In v2, add our own checks (account age, past orders, cards or devices shared by many accounts).
3. **Queue order:** everyone who joins before the start, or within 30 s after it, gets a **random** place. People who join later go to the end, first come, first served.
4. **How it connects to us:** the waiting room gives each person it lets in a signed entry pass (user, drop, valid 10 min). Our gateway checks the pass itself, with no call to the vendor. Our admission controller (ADR-0007) sets how fast the waiting room lets people in, through the vendor's API.

## What we didn't pick

| Option | Why not |
|---|---|
| Build our own waiting room now | months of work to handle 333K requests/s worldwide and survive our own outages; not our product; not ready for the next drop |
| Run the waiting room on our own servers | every "is it my turn?" check would still reach us, so it falls over exactly when we need it |
| Strict first come, first served | the fastest networks and bots win at the first second; needs one global ordered queue |
| Pure lottery (no queue) | fair against bots, but people who join later have no idea where they stand |
| Build bot detection ourselves first | vendors see bots across thousands of sites and update daily; we would start years behind |

## Consequences

- **Good:**
  - live in days
  - the CDN answers ~97% of drop traffic
  - our servers stay protected even when our region is down
  - bots gain nothing by being fastest at the first second
- **Bad: we pay the vendor per user,** and a vendor outage could stop the drop. **Fallback:** our gateway caps traffic at 30K requests/s (06-scalability-and-fault-tolerance.md, section 8). Everyone above that gets "try again".
- **Bad: we can't see how the vendor scores bots.** **Fallback:** before orders ship, we review suspicious ones after the drop. In v2, our own checks are added on top.
- **The vendor is easy to replace.** Switching vendors only changes who signs the entry pass. Our services stay the same.

## When to revisit

- The vendor's cost goes past an agreed share of drop revenue.
- We need rules the vendor can't support (for example, loyal customers first).
- A vendor outage causes a failed drop.
- In v2, we compare the vendor's cost with building our own waiting room.
