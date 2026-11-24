# ADR-0008: Save the order before charging the card

## Context

A **Hold** (reservation) is the first click: the buyer gets one specific unit kept for them for 3 min while they pay. If they don't pay in time, the unit goes back for someone else.

At the last drop, some people were charged but got no order. The old flow charged the card first and saved the order after it. Any failure in between (a crash, no free database connection, a deploy) left money taken with no record of what it was for. A PSP timeout made it worse: the card may have been charged even though we saw an error, and a retry without a key charged it again.

## Decision

1. **Before any charge,** the checkout service freezes the hold in Redis (so it can't expire), then saves **4 rows in one Postgres transaction** (all saved or none):
   - **order**: PAYMENT_PENDING
   - **allocation**: which unit goes to which user. The unique rules block a double sale: one unit is sold once, one unit per person.
   - **payment**: PENDING, one per reservation (`UNIQUE (reservation_id)`)
   - **outbox job**: "charge this card"
2. **Only the payment worker calls the PSP.** It uses the **reservation ID as the PSP key**, so a retry never charges twice. Each charge also carries our order, user, drop and unit IDs, so a missing order can be rebuilt from the PSP's records.
3. **A pending payment never times out by itself.** It ends only with an answer from the PSP:
   - the worker's own call
   - the PSP webhook
   - or the fix-up job, which asks the PSP about any payment still pending after 2 min
4. **Each state change happens only if the row is still in the state we expect.** For example, PENDING → PAID only if it's still PENDING. A late or repeated answer then changes nothing.
5. **A charge that arrives after we marked the order FAILED is refunded automatically,** with its own PSP key, so the refund also happens only once.

## What we didn't pick

| Option | Why not |
|---|---|
| Charge first, then save the order (old design) | a crash in between leaves a charge with no order |
| Charge and save the order in one shared transaction across both systems | the PSP can't join our database transactions; no such option exists between us and the PSP |
| Charge first, refund if saving fails | works, but every failure becomes a charge and then a refund on the user's card statement. Saving first avoids most of them |
| Mark the payment failed when the PSP times out | after a timeout we don't know if the card was charged. Failing it gives the unit away while the user may have paid |

## Consequences

- **Good:**
  - an order always exists before money moves. "Charged without an order" becomes at worst "order waiting for its payment result", which the fix-up job settles within ~3 min
  - Postgres's unique rules are checked before the charge, so a unit given out twice by Redis never costs anyone money
  - retries are safe everywhere, because of the PSP key and the state checks
- **Bad: one Postgres transaction per Pay** (≤ 800/s). **Fallback:** well within what Postgres can handle (02-scale-and-numbers.md, point 9).
- **Bad: a PAYMENT_PENDING order waits until the PSP answers.** If the PSP is down, the unit stays frozen for that time, so no one else can buy it. **Fallback:** a declined card frees the unit at once, and the fix-up job settles anything without an answer.

## When to revisit

- We want to approve the card first and take the money later (for example, when the item ships). The same order of steps works, with one extra state: AUTHORISED.
