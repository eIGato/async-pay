import logging
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from async_pay.api.deps import (
    get_session,
    require_api_key,
    require_idempotency_key,
)
from async_pay.schemas import (
    CreatePaymentRequest,
    CreatePaymentResponse,
    PaymentResponse,
)
from async_pay.services import payments as payment_service

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=CreatePaymentResponse,
)
async def create_payment(
    request: CreatePaymentRequest,
    idempotency_key: str = Depends(require_idempotency_key),
    session: AsyncSession = Depends(get_session),
) -> CreatePaymentResponse:
    try:
        payment, created = await payment_service.create_or_get_payment(
            session, idempotency_key, request
        )
    except payment_service.IdempotencyConflictError as exc:
        logger.warning(
            "idempotency key reused with a different body key=%s payment_id=%s",
            idempotency_key,
            exc.payment_id,
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key was already used with a different request body",
        ) from exc
    if created:
        logger.info("payment created id=%s idempotency_key=%s", payment.id, idempotency_key)
    else:
        logger.info(
            "payment replayed via idempotency key id=%s idempotency_key=%s",
            payment.id,
            idempotency_key,
        )
    return CreatePaymentResponse(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


@router.get("/{payment_id}", response_model=PaymentResponse)
async def get_payment(
    payment_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> PaymentResponse:
    payment = await payment_service.get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="payment not found",
        )
    return PaymentResponse.model_validate(payment)
