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

## DLQ and retry

Two layers of retry protect the consumer:

1. **RabbitMQ level.** `payments.new` is a quorum queue with
   `x-delivery-limit=3`. If the handler raises an unhandled exception the
   message is nacked without requeue (`retry=False` on the FastStream
   subscriber) and RabbitMQ dead-letters it into `payments.dlx` →
   `payments.dead`. Consumer crashes before ack are also bounded by the
   delivery limit so a poison message cannot loop forever.
2. **Webhook level.** Webhook delivery owns its retry loop: up to 3 attempts
   with exponential backoff (1s, 2s, 4s). Webhook failures are non-fatal —
   after the final attempt the error is logged and the RabbitMQ message is
   still acked, because the payment has already been finalised in the DB.

Dead-lettered messages can be inspected via the `payments.dead` queue in the
RabbitMQ management UI. The consumer also attaches a lightweight subscriber
to that queue so every dead-letter is surfaced in the application log.
