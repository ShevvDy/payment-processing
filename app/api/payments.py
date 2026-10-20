import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import verify_api_key
from app.db.session import get_session
from app.schemas.payment import PaymentCreate, PaymentCreatedResponse, PaymentRead
from app.services.payments import IdempotencyConflictError, PaymentService

router = APIRouter(
    prefix="/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
)


def get_payment_service(session: AsyncSession = Depends(get_session)) -> PaymentService:
    return PaymentService(session)


@router.post(
    "/",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentCreatedResponse,
    responses={409: {"description": "Idempotency-Key передан с другим телом запроса"}},
)
async def create_payment(
    data: PaymentCreate,
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=255),
    service: PaymentService = Depends(get_payment_service),
) -> PaymentCreatedResponse:
    try:
        payment = await service.create(data, idempotency_key)
    except IdempotencyConflictError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key передан с другим телом запроса",
        )
    return PaymentCreatedResponse(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


@router.get("/{payment_id}", response_model=PaymentRead)
async def get_payment(
    payment_id: uuid.UUID,
    service: PaymentService = Depends(get_payment_service),
) -> PaymentRead:
    payment = await service.get(payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Платеж не найден")
    return PaymentRead.model_validate(payment)
