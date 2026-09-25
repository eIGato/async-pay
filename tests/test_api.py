from uuid import uuid4

import pytest

API_KEY = "test-key"


async def test_create_payment_requires_api_key(client):
    response = await client.post(
        "/api/v1/payments",
        json={"amount": "10.00", "currency": "RUB"},
    )
    assert response.status_code == 401


async def test_create_payment_rejects_wrong_api_key(client):
    response = await client.post(
        "/api/v1/payments",
        json={"amount": "10.00", "currency": "RUB"},
        headers={"X-API-Key": "nope"},
    )
    assert response.status_code == 401


async def test_create_payment_requires_idempotency_key(client):
    response = await client.post(
        "/api/v1/payments",
        json={"amount": "10.00", "currency": "RUB"},
        headers={"X-API-Key": API_KEY},
    )
    assert response.status_code == 400


async def test_create_payment_returns_202(client):
    idem = str(uuid4())
    response = await client.post(
        "/api/v1/payments",
        json={
            "amount": "100.50",
            "currency": "RUB",
            "description": "test order",
            "metadata": {"order_id": "123"},
            "webhook_url": "https://example.com/webhook",
        },
        headers={"X-API-Key": API_KEY, "Idempotency-Key": idem},
    )
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "pending"
    assert body["payment_id"]
    assert body["created_at"]


async def test_create_payment_is_idempotent(client):
    idem = str(uuid4())
    payload = {"amount": "10.00", "currency": "RUB"}
    headers = {"X-API-Key": API_KEY, "Idempotency-Key": idem}

    first = await client.post("/api/v1/payments", json=payload, headers=headers)
    second = await client.post("/api/v1/payments", json=payload, headers=headers)

    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["payment_id"] == second.json()["payment_id"]


async def test_get_payment_returns_404_when_missing(client):
    response = await client.get(
        f"/api/v1/payments/{uuid4()}",
        headers={"X-API-Key": API_KEY},
    )
    assert response.status_code == 404


async def test_get_payment_returns_full_object(client):
    idem = str(uuid4())
    create = await client.post(
        "/api/v1/payments",
        json={
            "amount": "100.50",
            "currency": "USD",
            "description": "order",
            "metadata": {"k": "v"},
        },
        headers={"X-API-Key": API_KEY, "Idempotency-Key": idem},
    )
    payment_id = create.json()["payment_id"]

    response = await client.get(
        f"/api/v1/payments/{payment_id}",
        headers={"X-API-Key": API_KEY},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["id"] == payment_id
    assert body["amount"] == "100.50"
    assert body["currency"] == "USD"
    assert body["status"] == "pending"
    assert body["metadata"] == {"k": "v"}
    assert body["idempotency_key"] == idem


async def test_invalid_amount_returns_422(client):
    response = await client.post(
        "/api/v1/payments",
        json={"amount": "-5.00", "currency": "RUB"},
        headers={"X-API-Key": API_KEY, "Idempotency-Key": str(uuid4())},
    )
    assert response.status_code == 422


BASE_BODY = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "Order #123",
    "metadata": {"order_id": "123"},
    "webhook_url": "https://example.com/hook",
}
AUTH = {"X-API-Key": "test-key"}


def _headers(key: str = "conflict-key") -> dict:
    return {**AUTH, "Idempotency-Key": key}


async def test_replaying_the_same_body_is_idempotent(client):
    first = await client.post("/api/v1/payments", json=BASE_BODY, headers=_headers("same"))
    second = await client.post("/api/v1/payments", json=BASE_BODY, headers=_headers("same"))

    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["payment_id"] == second.json()["payment_id"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("amount", "999.99"),
        ("currency", "USD"),
        ("description", "Order #456"),
        ("metadata", {"order_id": "456"}),
        ("webhook_url", "https://example.com/other"),
    ],
)
async def test_reusing_idempotency_key_with_a_different_body_returns_409(client, field, value):
    key = f"conflict-{field}"
    first = await client.post("/api/v1/payments", json=BASE_BODY, headers=_headers(key))
    assert first.status_code == 202

    conflicting = await client.post(
        "/api/v1/payments", json={**BASE_BODY, field: value}, headers=_headers(key)
    )

    assert conflicting.status_code == 409
    assert "different request body" in conflicting.json()["detail"]

    # The original payment is untouched.
    stored = await client.get(f"/api/v1/payments/{first.json()['payment_id']}", headers=AUTH)
    assert stored.json()["description"] == BASE_BODY["description"]


async def test_equivalent_amount_is_not_a_conflict(client):
    """100.5 and 100.50 are the same decimal, not a changed body."""
    key = "amount-scale"
    first = await client.post("/api/v1/payments", json=BASE_BODY, headers=_headers(key))
    second = await client.post(
        "/api/v1/payments", json={**BASE_BODY, "amount": "100.5"}, headers=_headers(key)
    )

    assert second.status_code == 202
    assert second.json()["payment_id"] == first.json()["payment_id"]


async def test_health_reports_ok(client):
    response = await client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "database": "ok"}


async def test_health_needs_no_api_key(client):
    assert (await client.get("/health")).status_code == 200


async def test_health_reports_503_when_the_database_is_down(app, client, monkeypatch):
    class DeadFactory:
        def __call__(self):
            return self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def execute(self, *args, **kwargs):
            raise RuntimeError("database is gone")

    app.state.session_factory = DeadFactory()

    response = await client.get("/health")

    assert response.status_code == 503
    assert response.json()["database"] == "unreachable"
