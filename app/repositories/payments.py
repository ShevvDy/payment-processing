import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Payment, PaymentStatus


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, payment_id: uuid.UUID) -> Optional[Payment]:
        return await self._session.get(Payment, payment_id)

    async def get_by_idempotency_key(self, idempotency_key: str) -> Optional[Payment]:
        result = await self._session.execute(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        return result.scalar_one_or_none()

    def add(self, payment: Payment) -> None:
        self._session.add(payment)

    async def complete(
        self, payment_id: uuid.UUID, status: PaymentStatus, processed_at: datetime
    ) -> bool:
        """Фиксирует результат, только если платёж ещё в pending. Возвращает True, если обновили."""
        result = await self._session.execute(
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=status, processed_at=processed_at)
        )
        return result.rowcount == 1
