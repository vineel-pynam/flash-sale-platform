# ADR-0004: Hold answers at once, payment runs in the background

## Context

A **Hold** (reservation) is the first click: the buyer gets one specific unit kept for them for 3 min while they pay. If they don't pay in time, the unit goes back for someone else.

In the old design, the user's request waited while we called the PSP. The PSP's slowness (up to 2 s, p99), its timeouts and its 1,000 payments/s limit became our slowness, our errors and our limit. While waiting, each request kept a server thread and a database connection busy, so other pages slowed down too. The brief allows the order confirmation to arrive a little later.

## Decision

1. **Hold answers at once.** When the user clicks Hold, the answer comes back within 500 ms (p99), and it's the real answer:
   - "you got unit U012345, kept for you until 10:03", or
   - "sold out", or
   - "you already have a hold".

   This only uses Redis, so it's fast even at peak. The user knows straight away whether to go to payment.
2. **Pay answers at once** with `202` ("payment started"). It does not wait for the card to be charged. Before answering, it does two quick things:
   - **Freezes the hold in Redis.** A hold normally expires after 3 min. Freezing it tells the hold expiry job to skip it, so the unit is not given to someone else while the card is being charged. If the payment fails, the unit is put back.
   - **Saves 4 rows in Postgres in one transaction** (all saved or none, ADR-0008):
     - **order**: "payment pending"
     - **allocation**: which unit goes to which user. The unique rules block a double sale here.
     - **payment**: "pending"
     - **outbox job**: "charge this card"

   Both take a few ms. Charging the card happens later, in the background (point 3).
3. **The card is charged in the background** by payment workers reading payment jobs from Kafka (ADR-0003). They protect us from a slow or failing PSP in four ways:
   - at most 800 new charges/s; the PSP's other 200/s is kept for retries
   - a 2.5 s timeout on each call
   - their own fixed number of connections, so a slow PSP can't use up resources the rest of the system needs
   - a circuit breaker that stops calling the PSP when it keeps failing
4. **The browser checks the status** (`GET /v1/reservations/{id}`) every 1–2 s until it shows PAID or FAILED, usually within 1–3 s. Pushing the result to the browser (server-sent events) can be added later.
5. **The order worker** confirms the order and sends the email in the background, normally within 30 s.

## What we didn't pick

| Option | Why not |
|---|---|
| Do everything inside the user's request | the PSP's slowness and limit become ours again; this is what broke the last drop |
| Call the PSP inside the request with a short timeout | after a timeout we don't know if the card was charged, so the user may see an error for a payment that went through |
| Make Hold run in the background too | the user wouldn't know if they got a unit, so they'd keep clicking and checking, adding more load |

## Consequences

- **Good:**
  - a slow PSP makes jobs wait in Kafka; users don't see errors
  - the PSP's limit is enforced in one place (the payment workers)
  - the servers that answer users never wait on the PSP
  - after a worker crash or a PSP timeout, the job is retried in the background with the same PSP key, so the card is never charged twice
- **Bad: the user sees "processing payment",** and the browser must keep checking. **Fallback:** it usually takes 1–3 s, so most users barely notice.
- **Bad: more parts to build and run** (outbox sender, payment worker, order worker, fix-up job).

## When to revisit

- The PSP offers fast, guaranteed answers with no limit at our scale (unlikely).
- A different kind of sale needs an instant confirmation page. The Hold design stays the same; only the screen changes.
