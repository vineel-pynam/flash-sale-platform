# Architecture Decision Records

Each record has the same parts: Context → Decision → What we didn't pick → Consequences → When to revisit.

| ADR | Decision | Problem from the last drop it fixes |
|---|---|---|
| [0001](0001-buy-waiting-room-and-bot-protection.md) | Buy the waiting room and bot protection; random order for everyone who joins by 30 s after the start, first come, first served after that | 25 s p99 (too much traffic reached our servers) |
| [0002](0002-redis-inventory-gate-postgres-source-of-truth.md) | Redis decides holds (lists of unit IDs); Postgres unique rules keep the truth | same seat sold twice; database overloaded |
| [0003](0003-managed-kafka-for-order-pipeline.md) | Managed Kafka split by order ID; jobs sent only through the outbox; workers skip duplicates | charged without an order |
| [0004](0004-sync-reservation-async-payment-and-confirmation.md) | Hold answers at once; payment and confirmation run in the background | 25 s p99; database out of connections |
| [0005](0005-single-writer-home-region-per-drop.md) | One home region sells each drop | same seat sold twice (across regions) |
| [0006](0006-prewarm-scheduled-scale-on-leading-signals.md) | Start servers before planned drops; scale on requests, not CPU | 25 s p99; database out of connections |
| [0007](0007-admission-rate-derived-from-psp-limit.md) | Let people in only as fast as the PSP can charge them, and never more than free units | 25 s p99; PSP overloaded |
| [0008](0008-create-order-before-charging.md) | Save the order, unit, payment and job before charging the card | charged without an order; charged twice |
