# async-pay

Asynchronous payment processing microservice built on FastAPI, SQLAlchemy 2.0
async, RabbitMQ (via FastStream) and PostgreSQL.

## Architecture

The service is split into two processes that share a Postgres database and a
RabbitMQ cluster:

* **API (`async_pay.api`)** — FastAPI application that accepts `POST
  /api/v1/payments` requests, writes the payment row and a corresponding row
  into the `outbox` table in a single DB transaction, and returns `202
  Accepted` immediately. A background asyncio task started in the `lifespan`
  of the app drains the outbox into RabbitMQ every 2 seconds.
* **Consumer (`async_pay.consumer`)** — FastStream worker subscribed to the
  `payments.new` queue. Each message triggers a simulated gateway interaction
  (a 2–5 s sleep, 90% success), the payment row is finalised, and — if a
  `webhook_url` was provided — an HTTP POST webhook is delivered with
  exponential backoff (1s, 2s, 4s) over up to 3 attempts.

RabbitMQ topology is declared at startup by both processes (idempotent via
`aio-pika`): a direct exchange `payments` with a quorum queue `payments.new`
configured with `x-delivery-limit=3` and dead-letter routing into a second
direct exchange `payments.dlx` and queue `payments.dead`.

## Running

```bash
make up           # builds and starts postgres, rabbitmq, api, consumer
make logs         # stream logs
make down         # stop everything
make clean        # stop and wipe the postgres volume
```

The API then listens on http://localhost:8000 and the RabbitMQ management UI
on http://localhost:15672 (guest/guest).

Host ports are overridable, for when 5432 or 8000 is already taken:

```bash
API_PORT=18000 POSTGRES_PORT=15432 RABBITMQ_MGMT_PORT=25672 make up
```

## Local development

```bash
make install     # pip install -e ".[dev]"
make hooks       # install the pre-commit hooks
make test        # run the pytest suite
make fmt         # ruff format + ruff check --fix
```

## API examples

Create a payment:

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: secret-api-key" \
  -H "Idempotency-Key: 5dce5a3e-4e6b-4c8b-9f40-9f38f9a4d111" \
  -H "Content-Type: application/json" \
  -d '{
    "amount": "100.50",
    "currency": "RUB",
    "description": "Order #123",
    "metadata": {"order_id": "123"},
    "webhook_url": "https://example.com/webhook"
  }'
```

Response (`202 Accepted`):

```json
{
  "payment_id": "b1d6e9a2-...-...",
  "status": "pending",
  "created_at": "2026-04-17T12:00:00+00:00"
}
```

Fetch a payment:

```bash
curl -s http://localhost:8000/api/v1/payments/<payment_id> \
  -H "X-API-Key: secret-api-key" | jq
```

Health probe (unauthenticated, used by the compose healthcheck):

```bash
curl -s http://localhost:8000/health | jq   # {"status":"ok","database":"ok"}
```

Returns `503` with `{"status":"unhealthy","database":"unreachable"}` when the
database cannot be reached, so `consumer` waits for a genuinely ready API
rather than merely a started container.

## Idempotency

`Idempotency-Key` is required on `POST /api/v1/payments` and is stored on the
payment under a unique index:

* **Same key, same body** — `202` with the original `payment_id`; no second
  payment and no second outbox event. A client that retries after a timeout
  gets the result of its first call.
* **Same key, different body** — `409 Conflict`. Silently returning the old
  payment would hide a real client bug (a reused key for a different charge),
  and creating a new one would defeat the key entirely. The comparison is on
  the business fields — amount, currency, description, metadata, webhook URL —
  so `100.5` and `100.50` are the same request, as are metadata objects that
  differ only in key order.
* **Concurrent duplicates** — the unique index is the arbiter: the losing
  transaction catches the `IntegrityError`, re-reads the winner's row and
  replays it through the same comparison.

## Outbox pattern

The API never talks to RabbitMQ inline with the request. Instead, writing a
payment and enqueueing the "payment created" event are performed in a single
database transaction by inserting into the `payments` and `outbox` tables
together. A separate asyncio task polls `outbox` every 2 seconds, publishes
each row to RabbitMQ, and marks it as `published_at = now()` on success.

Why:

* **Atomicity.** Without the outbox, the API would either commit to DB then
  publish (risking a lost event if RabbitMQ is unreachable) or publish then
  commit (risking a phantom event for a payment that never persisted).
* **Retry for free.** A publish failure leaves `published_at` NULL, so the
  next tick will pick the row up again. Duplicates are tolerated because the
  consumer re-checks the payment's status and bails out if it is no longer
  `pending`.
* **Back-pressure isolation.** RabbitMQ outages cannot bring down the API.

Each tick claims its batch with `SELECT ... FOR UPDATE SKIP LOCKED` and holds
the row locks until the `published_at` update commits, so the API can be scaled
to several replicas — each with its own publisher task — without two of them
publishing the same event. Postgres serialises the claim; a replica that finds
the rows locked simply skips them and takes the next batch.

## DLQ and retry

Two layers of retry protect the consumer:

1. **Message level — 3 attempts, then DLQ.** `payments.new` is a quorum queue
   with `x-delivery-limit=3`. The subscriber acks manually
   (`ack_policy=AckPolicy.MANUAL`) and `handle_message` decides each message's
   fate: ack on success, nack-with-requeue while attempts remain, and `reject`
   on the last one — `requeue=False` is what routes it through `payments.dlx`
   into `payments.dead`.

   The final rejection is deliberate rather than left to `x-delivery-limit`:
   RabbitMQ dead-letters a message once its delivery count *exceeds* the limit,
   so a limit of 3 buys 4 deliveries. Rejecting the third attempt ourselves
   makes "3 attempts" mean exactly three. The queue keeps the limit declared as
   a backstop for the case this code never gets to run — a consumer that crashes
   or is killed before acking — so a poison message still cannot loop forever.

   Between attempts the handler sleeps with exponential backoff
   (`consume_retry_base_delay` × 1, 2, 4 → 1s, 2s, 4s), so a transient outage
   downstream is not hammered by immediate redeliveries. The attempt number is
   read from the `x-delivery-count` header that quorum queues stamp on every
   redelivery; no attempt counter is kept in the payload.

   Failures a retry cannot fix are not retried at all: a payload without a
   usable `payment_id`, or an id with no matching row, is logged and acked.
2. **Webhook level.** Webhook delivery owns its retry loop: up to
   `webhook_max_attempts` attempts (default 3) with exponential backoff
   (`webhook_retry_base_delay` × 1, 2, 4 → 1s, 2s, 4s). Webhook failures are
   non-fatal — after the final attempt the error is logged and the RabbitMQ
   message is still acked, because the payment has already been finalised in
   the DB. Retrying the message here would re-run the whole handler, and the
   idempotency guard would simply skip the already-finalised payment.

Note that a *simulated gateway decline* (the 10% path) is a business outcome,
not a failure: the payment is finalised as `failed`, the webhook reports that
status and the message is acked. Only infrastructure errors — an unreachable
database, malformed payloads, bugs — feed the retry/DLQ machinery above.

The consumer deliberately does **not** subscribe to `payments.dead`: a DLQ that
is consumed is a DLQ that is empty, and the point of the queue is to retain what
failed until somebody looks at it. Inspect it without draining it with

```bash
make dlq          # docker compose exec consumer python -m async_pay.tools.dlq
```

which `basic.get`s the messages, prints each body together with its `x-death`
history, and nacks them all back onto the queue. The queue is also visible in
the RabbitMQ management UI at http://localhost:15672.

## Verifying the retry path by hand

The 10% simulated decline is a business outcome, so to exercise the retry and
DLQ machinery you need a message the handler genuinely cannot process. Publish
one with an unusable id straight onto the exchange:

```bash
docker compose exec -T consumer python - <<'EOF'
import asyncio, json, aio_pika
from async_pay.config import get_settings

async def main():
    s = get_settings()
    conn = await aio_pika.connect_robust(s.rabbitmq_url)
    ch = await conn.channel()
    ex = await ch.get_exchange(s.payments_exchange)
    await ex.publish(
        aio_pika.Message(json.dumps({"payment_id": "not-a-uuid"}).encode()),
        routing_key=s.payments_routing_key,
    )
    await conn.close()

asyncio.run(main())
EOF

docker compose logs consumer | grep attempt
# attempt 1/3, requeueing in 1.0s
# attempt 2/3, requeueing in 2.0s
# attempt 3/3, dead-lettering

make dlq
# [1] message_id=... deaths=[{'count': 1, 'reason': 'rejected', ...}] body={"payment_id": "not-a-uuid"}
```
