import hashlib
import json
import uuid
from typing import Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.broker.topology import PAYMENT_CREATED_EVENT, PAYMENTS_NEW_ROUTING_KEY
from app.db.models import OutboxEvent, Payment
from app.repositories.outbox import OutboxRepository
from app.repositories.payments import PaymentRepository
from app.schemas.payment import PaymentCreate


class IdempotencyConflictError(Exception):
    """Ключ идемпотентности уже использован с другим телом запроса."""


def _request_hash(data: PaymentCreate) -> str:
    # Хешируем нормализованное тело, чтобы порядок ключей и формат чисел не влияли на результат
    canonical = json.dumps(data.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


class PaymentService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)

    async def get(self, payment_id: uuid.UUID) -> Optional[Payment]:
        return await self._payments.get(payment_id)

    async def create(self, data: PaymentCreate, idempotency_key: str) -> Payment:
        """Создаёт платёж или возвращает ранее созданный по тому же ключу идемпотентности"""
        request_hash = _request_hash(data)

        existing = await self._payments.get_by_idempotency_key(idempotency_key)
        if existing is not None:
            return self._ensure_same_request(existing, request_hash)

        payment = Payment(
            id=uuid.uuid4(),
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            metadata_=data.metadata,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            webhook_url=str(data.webhook_url),
        )
        self._payments.add(payment)
        # Событие пишется в той же транзакции, что и платеж
        self._outbox.add(
            OutboxEvent(
                aggregate_id=payment.id,
                event_type=PAYMENT_CREATED_EVENT,
                routing_key=PAYMENTS_NEW_ROUTING_KEY,
                payload={"payment_id": str(payment.id)},
            )
        )

        try:
            await self._session.commit()
        except IntegrityError:
            # Параллельный запрос с тем же ключом успел закоммитить раньше
            await self._session.rollback()
            existing = await self._payments.get_by_idempotency_key(idempotency_key)
            if existing is None:
                raise
            return self._ensure_same_request(existing, request_hash)

        await self._session.refresh(payment)
        return payment

    @staticmethod
    def _ensure_same_request(payment: Payment, request_hash: str) -> Payment:
        if payment.request_hash != request_hash:
            raise IdempotencyConflictError
        return payment
