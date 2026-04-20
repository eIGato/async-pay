from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from async_pay.models import Currency, PaymentStatus


class CreatePaymentRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    amount: Decimal = Field(..., gt=Decimal("0"), max_digits=18, decimal_places=2)
    currency: Currency
    description: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl | None = None


class CreatePaymentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: UUID
    amount: Decimal
    currency: Currency
    description: str | None = None
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        validation_alias="meta",
    )
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str | None = None
    created_at: datetime
    processed_at: datetime | None = None


class WebhookPayload(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID
    status: PaymentStatus
    processed_at: datetime
