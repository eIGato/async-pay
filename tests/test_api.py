from uuid import uuid4

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
