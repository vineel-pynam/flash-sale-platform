# ADR-0003: Managed Kafka for payment jobs and order events

## Context

Charging the card must happen outside the user's request, so a slow PSP never slows users down. The jobs need a safe place to wait until a worker picks them up.

- At peak there are ≤ 1,000 payment jobs/s (~800 payments/s), plus a few order events for each payment: a few thousand messages/s at most.
- More than one worker reads the same events: the payment worker charges the card, the order worker confirms the order and sends the email. Later, others (analytics, shipping) can read them too.
- Jobs for one order must run in order (charge before confirm).
- After a bug, we must be able to process past events again.

## Decision

1. Use **managed Kafka** (Amazon MSK or Confluent Cloud).
   - Every message is kept on 3 machines, one per availability zone.
   - Kafka says "saved" only after at least 2 of them have it.
2. **Topics:**
   - 2 main topics: payment jobs, order events
   - 3 retry topics for payment jobs (5 s, 30 s, 2 min)
   - 1 dead-letter topic for jobs that can never work (bad data)

   Each topic is split into **48 parts (partitions) by order ID**, so all jobs for one order land in the same part, in order.
3. Services send messages **only through the outbox table** in Postgres. The order and its job are saved in one step; the outbox sender then moves the job to Kafka. No service saves to Postgres and sends to Kafka as two separate steps.
4. Kafka may deliver a message twice, so **each worker skips messages it has already done**. It saves the message ID in `processed_messages` in the same step as its work. It tells Kafka "done" only after the work is saved.

## What we didn't pick

| Option | Why not |
|---|---|
| Amazon SQS (standard queue) | no order; each message goes to only one reader, so every new reader needs its own copy of the queue; can't process past messages again |
| Amazon SQS FIFO | keeps order, but can't process past messages again, has speed limits per group, and still needs a copy of the queue per reader |
| Split by item (drop) instead of order ID | a drop is one item, so every message lands in one part with one worker; no parallel work |
| Run Kafka ourselves | same Kafka, but running the machines, upgrades and failover across 3 availability zones is a full-time job |
| Redis Streams | fast, but Redis can lose the last ~1 s of data in a failover, which is the weakest part of our design |
| Send straight to Kafka (no outbox) | a crash after saving the order but before sending loses the job, which is how "charged without an order" happens |

## Consequences

- **Good:**
  - a safe place for jobs to wait between Postgres and the PSP
  - past messages can be processed again after a bug
  - many readers on the same messages
  - jobs for one order stay in order, and the load is spread over 48 parts
  - if Kafka is down, jobs wait in the outbox table and are sent later; nothing is lost
- **Bad: more cost and more to learn than SQS** (parts, worker groups, backlog). **Fallback:** a managed service, plus dashboards and alerts on the backlog.
- **Bad: a message can arrive twice.** **Fallback:** every worker skips messages it has already done, and the payment worker uses the reservation ID as the PSP key, so a repeat never charges twice. We don't use Kafka's "exactly once" mode, because it only covers Kafka itself, not the PSP or Postgres.

## When to revisit

- Messages grow far beyond what 48 parts can handle. Adding parts needs a plan, because orders move to new parts.
- The company standardises on another tool (for example Kinesis).
- The outbox sender's polling puts real load on Postgres. Then switch it to CDC (Debezium reads Postgres's change log).
