# Entities, APIs & Schema

## 1. Entities

| Entity | What it is |
|---|---|
| **Drop** | one sale event: an item, a start time, a stock count |
| **Unit** | one sellable thing. Every unit has an ID: a seat (`A-14`) or a plain item (`U000042`). So the same design works for seats and for plain stock. |
| **User** | a buyer |
| **Hold** (reservation) | "this unit is yours for 3 min, go pay" |
| **Order** | the purchase record, created when the user pays |
| **Payment** | the charge at the PSP for an order |
| **Refund** | money given back (for example, a charge that arrived too late) |

## 2. APIs

| API | What it does | Answers |
|---|---|---|
| `GET /v1/drops/{drop}/availability` | "how many left?" (cached) | `200 { available }` |
| `POST /v1/drops/{drop}/reservations` | **Hold** a unit | `201` held, `200` already held, `410` sold out, `409` a click is already in progress, `503` try again in 1 s |
| `POST /v1/reservations/{id}/payment` | **Pay** with a card token | `202` payment started, `410` hold expired, `409` unit already taken or already bought, `503` try again in 2 s |
| `GET /v1/reservations/{id}` | check status | `200 { state: HELD / PAYMENT_PENDING / PAID / FAILED / REFUNDED }` |

**Every Hold and Pay call sends two headers:**
- `X-Admission-Token`: the entry pass from the waiting room (who you are, which drop, valid 10 min)
- `Idempotency-Key`: if the same request is sent again, we return the first answer and never do it twice

**Errors on both:** `401` missing or bad entry pass, `422` the same Idempotency-Key reused with a different request.

## 3. Redis (fast, temporary)

Stock is split into **50 buckets** (`00`–`49`), about 1,000 units each. The part inside `{ }` decides which Redis shard a key lives on, so each bucket's keys stay together on one shard.

### Keys per bucket (bucket `07` shown)

**`inv:{drop-1:07}:units`**: list of free unit IDs

| Type | Example | Lives for |
|---|---|---|
| list | `U000042`, `U000043`, ... | the drop |

**`inv:{drop-1:07}:hold:<reservation_id>`**: one hold

| Field | Example | Meaning |
|---|---|---|
| `user` | `user-123` | who holds it |
| `unit` | `U000042` | which unit |
| `state` | `HELD` / `FROZEN` | FROZEN = payment in progress, so it can't expire |
| `exp` | `1732350000000` | when the hold ends (3 min after the Hold) |
| `frozen_at` | `1732349950000` | when payment started |

Type: hash. Lives until paid, expired or declined.

**`inv:{drop-1:07}:expiry`**: holds in this bucket, sorted by end time

| Type | Member | Score |
|---|---|---|
| sorted set | reservation ID | end time |

The hold expiry job (section 6) reads this every second and returns expired units to the list.

**`inv:{drop-1:12}:user:<user_id>`**: one hold per person

**Home bucket:** each user has a fixed bucket, `hash(user_id) % 50`. For example, `user-123` always maps to bucket `12`. Their user key always lives there. This lets Redis check "already has a hold?" and take a unit in **one step**.

- Home bucket has stock → take a unit from it.
- Home bucket is empty → mark the key `pending:` and take a unit from another bucket (for example `41`). The hold then lives in bucket 41, and its ID starts with `41-`, so we can find it.

| Value | Meaning | Lives for |
|---|---|---|
| *(missing)* | no hold yet | |
| `pending:<reservation_id>` | searching other buckets | 5 s |
| `<reservation_id>` | holds a unit | 4 min; no limit once payment starts |
| `sold:<reservation_id>` | bought | forever |

### Keys per drop

| Key | Type | Meaning | Lives for |
|---|---|---|---|
| `inv:{drop-1}:nonempty` | set | which buckets still have stock | the drop |
| `inv:{drop-1}:soldout` | string | "nothing left right now" shortcut | 2 s |

### Other keys

| Key | Type | Meaning | Lives for |
|---|---|---|---|
| `idem:<user_id>:<key>` | string | the saved answer for a Hold or Pay request | 30 s while running, 24 h once done |

**Good to know:**
- Redis can be **rebuilt from Postgres** at any time (see section 5).

## 4. Postgres (the truth for money and ownership)

**drops**: one row per sale event

| Column | Type | Meaning |
|---|---|---|
| `event_id` | text, primary key | the drop (the same ID the other tables use) |
| `name` | text | item name |
| `price_cents` | bigint | price per unit |
| `total_units` | int | stock |
| `starts_at` | timestamp | when the sale opens |
| `hold_seconds` | int | how long a hold lasts (180) |
| `home_region` | text | where this drop's stock lives |
| `state` | text | SCHEDULED, LIVE, ENDED |

**units**: every sellable unit, created when the drop is set up

| Column | Type | Meaning |
|---|---|---|
| `event_id` + `unit_id` | text, primary key | one row per unit |
| `label` | text | what the buyer sees, for example "Seat A-14" (empty for plain items) |
| `bucket` | smallint | which Redis bucket the unit goes into (00–49) |

**orders**: one row per purchase

| Column | Type | Meaning |
|---|---|---|
| `order_id` | UUID, primary key | the order |
| `reservation_id` | text, unique | the hold it came from (one order per hold) |
| `event_id` | text | the drop |
| `user_id` | text | the buyer |
| `unit_id` | text | the unit bought |
| `amount_cents` | bigint | price |
| `state` | text | PAYMENT_PENDING, PAID, FAILED, REFUNDED |
| `created_at`, `updated_at` | timestamp | |

**allocations**: who owns which unit. **This table is what stops double selling.**

| Column | Type | Meaning |
|---|---|---|
| `event_id` + `unit_id` | text, **primary key** | **one owner per unit** |
| `user_id` | text | owner. **Unique with `event_id`**: one unit per person |
| `reservation_id` | text, unique | the hold it came from |
| `order_id` | UUID, links to orders | the order |
| `created_at` | timestamp | |

**payments**: one row per order

| Column | Type | Meaning |
|---|---|---|
| `reservation_id` | text, primary key | also the PSP's idempotency key, so **one charge per hold** |
| `order_id` | UUID, links to orders | the order |
| `amount_cents` | bigint | amount to charge |
| `payment_token` | text | card token from the PSP (never the card number) |
| `state` | text | PENDING, SUCCEEDED, DECLINED, REFUNDED |
| `psp_charge_id` | text | the PSP's ID for the charge |
| `attempts` | int | how many times we tried |
| `updated_at` | timestamp | |

**outbox**: jobs to do after the database save (for example, "charge this card")

| Column | Type | Meaning |
|---|---|---|
| `id` | bigserial, primary key | job number |
| `aggregate_id` | text | which order or hold it is about |
| `event_id` | text | which drop; a drop's worker only picks its own jobs |
| `event_type` | text | for example, PaymentRequested |
| `payload` | JSON | job details |
| `available_at` | timestamp | don't pick it up before this time (used for retry delays) |
| `attempts` | int | how many times tried |
| `processed_at` | timestamp | empty = still to do |
| `created_at` | timestamp | |

**refunds**: money to give back

| Column | Type | Meaning |
|---|---|---|
| `refund_id` | text, primary key | also the PSP's idempotency key, so a refund is never sent twice |
| `reservation_id` | text | which hold or charge |
| `reason` | text | why we refund |
| `sent_at` | timestamp | empty until the PSP accepts the refund |
| `created_at` | timestamp | |

**processed_messages**: stops a worker doing the same job twice (Kafka can deliver a message twice)

| Column | Type | Meaning |
|---|---|---|
| `consumer` + `message_id` | text, primary key | "this worker already handled this message" |
| `processed_at` | timestamp | when |

### Key rules in the database

| Table | Rule | Stops |
|---|---|---|
| allocations | primary key (`event_id`, `unit_id`) | the same unit sold twice |
| allocations | unique (`event_id`, `user_id`) | one person buying two units |
| orders | unique `reservation_id` | two orders for one hold |
| payments | primary key `reservation_id` | two charges for one hold |
| refunds | primary key `refund_id` | the same refund sent twice |
| processed_messages | primary key (`consumer`, `message_id`) | the same message handled twice |
| all tables | check on `state` | a made-up state from a bug |

If Redis ever makes a mistake, these rules make the database **reject** it.

**Indexes:** small indexes that cover only "still to do" rows:
- orders waiting for payment (fix-up job)
- outbox jobs not done yet (payment worker)
- refunds not sent yet (fix-up job)

**How the tables connect:**
- a drop has many units
- an order has one payment
- an order has one allocation, until the payment fails (then the allocation is deleted and the unit is free again)
- a payment has a refund only if needed

**When the user pays, we save 4 rows in one transaction:** order + allocation + payment + outbox. Either all 4 are saved, or none are.

## 5. Rebuilding Redis from Postgres

If Redis loses its data:
1. **Pause new Holds.**
2. **Free units** = all rows in `units` − units already in `allocations` → push them back into their buckets.
3. **Bought** = allocations whose order is PAID → set that user's key to `sold:` (they can't buy again).
4. **Being paid right now** = allocations whose order is PAYMENT_PENDING → re-create the hold as FROZEN and set the user's key to the reservation ID. Then success or failure works as normal.
5. **Held but not yet paid** is the only thing lost (it lived only in Redis, for 3 min). Those users must click Hold again.
6. **Resume Holds.**

## 6. Background jobs

These run on their own, not because of a user click.

| Name | What it does | How often | Name in code |
|---|---|---|---|
| **Hold expiry job** | finds holds whose 3 minutes are over and puts the unit back for sale | every 1 s | reaper |
| **Payment worker** | takes "charge this card" jobs, calls the PSP, saves the result (paid / declined / unknown) | all the time | payment worker |
| **Outbox sender** | reads new jobs from the `outbox` table and sends them to Kafka, so other services hear about them | every 50 ms | outbox relay |
| **Order worker** | after a payment succeeds: confirms the order, sends the email/notification | all the time | order consumer |
| **Fix-up job** | checks that our records, the PSP and Redis agree, and fixes any mismatch: asks the PSP about payments with no answer, resends refunds not yet sent, cleans up stuck holds | every 1 min | reconciler |

## 7. Full SQL schema

```sql
CREATE TABLE drops (
    event_id        TEXT        PRIMARY KEY,
    name            TEXT        NOT NULL,
    price_cents     BIGINT      NOT NULL CHECK (price_cents > 0),
    total_units     INT         NOT NULL CHECK (total_units > 0),
    starts_at       TIMESTAMPTZ NOT NULL,
    hold_seconds    INT         NOT NULL DEFAULT 180,
    home_region     TEXT        NOT NULL,
    state           TEXT        NOT NULL CHECK (state IN ('SCHEDULED', 'LIVE', 'ENDED'))
);

CREATE TABLE units (
    event_id        TEXT        NOT NULL REFERENCES drops (event_id),
    unit_id         TEXT        NOT NULL,
    label           TEXT,                                   -- "Seat A-14", empty for plain items
    bucket          SMALLINT    NOT NULL CHECK (bucket BETWEEN 0 AND 49),
    PRIMARY KEY (event_id, unit_id)
);

CREATE TABLE orders (
    order_id        UUID        PRIMARY KEY,
    reservation_id  TEXT        NOT NULL UNIQUE,            -- one order per hold
    event_id        TEXT        NOT NULL,
    user_id         TEXT        NOT NULL,
    unit_id         TEXT        NOT NULL,
    amount_cents    BIGINT      NOT NULL CHECK (amount_cents > 0),
    state           TEXT        NOT NULL CHECK (state IN ('PAYMENT_PENDING', 'PAID', 'FAILED', 'REFUNDED')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX orders_pending_idx ON orders (updated_at) WHERE state = 'PAYMENT_PENDING';

CREATE TABLE allocations (
    event_id        TEXT        NOT NULL,
    unit_id         TEXT        NOT NULL,
    user_id         TEXT        NOT NULL,
    reservation_id  TEXT        NOT NULL UNIQUE,
    order_id        UUID        NOT NULL REFERENCES orders (order_id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, unit_id),                        -- one owner per unit
    UNIQUE      (event_id, user_id),                        -- one unit per person
    FOREIGN KEY (event_id, unit_id) REFERENCES units (event_id, unit_id)
);

CREATE TABLE payments (
    reservation_id  TEXT        PRIMARY KEY,                -- also the PSP idempotency key
    order_id        UUID        NOT NULL REFERENCES orders (order_id),
    amount_cents    BIGINT      NOT NULL,
    payment_token   TEXT        NOT NULL,
    state           TEXT        NOT NULL CHECK (state IN ('PENDING', 'SUCCEEDED', 'DECLINED', 'REFUNDED')),
    psp_charge_id   TEXT,
    attempts        INT         NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE outbox (
    id              BIGSERIAL   PRIMARY KEY,
    aggregate_id    TEXT        NOT NULL,
    event_id        TEXT,                                   -- the drop
    event_type      TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    available_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts        INT         NOT NULL DEFAULT 0,
    processed_at    TIMESTAMPTZ
);

CREATE INDEX outbox_pending_idx ON outbox (event_id, event_type, id) WHERE processed_at IS NULL;

CREATE TABLE refunds (
    refund_id       TEXT        PRIMARY KEY,                -- also the PSP idempotency key
    reservation_id  TEXT        NOT NULL,
    reason          TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at         TIMESTAMPTZ                             -- empty until the PSP accepts it
);

CREATE INDEX refunds_unsent_idx ON refunds (created_at) WHERE sent_at IS NULL;

CREATE TABLE processed_messages (
    consumer        TEXT        NOT NULL,                   -- which worker
    message_id      TEXT        NOT NULL,                   -- Kafka message ID
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);
```
