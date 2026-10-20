import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumer.gateway import PaymentGateway
from app.consumer.webhook import WebhookSender
from app.db.models import PaymentStatus
from app.db.session import session_factory
from app.repositories.payments import PaymentRepository
from app.schemas.payment import PaymentWebhook

logger = logging.getLogger(__name__)


class PaymentNotFoundError(Exception):
    """Платёж из сообщения отсутствует в БД"""


class PaymentProcessor:
    """Обработка платежа: шлюз -> фиксация статуса -> webhook"""

    def __init__(self, gateway: PaymentGateway, webhook_sender: WebhookSender) -> None:
        self._gateway = gateway
        self._webhook_sender = webhook_sender

    async def process(self, payment_id: uuid.UUID) -> None:
        async with session_factory() as session:
            payments = PaymentRepository(session)
            payment = await payments.get(payment_id)
            if payment is None:
                raise PaymentNotFoundError(payment_id)

            if payment.status == PaymentStatus.PENDING:
                status = await self._gateway.charge()
                updated = await payments.complete(payment_id, status, datetime.now(timezone.utc))
                await session.commit()
                if not updated:
                    logger.info("Платёж %s уже обработан параллельно, результат шлюза отброшен", payment_id)
                await session.refresh(payment)
                logger.info("Платёж %s обработан: %s", payment_id, payment.status.value)

            webhook = PaymentWebhook.model_validate(payment)
            webhook_url = payment.webhook_url

        await self._webhook_sender.send(webhook_url, webhook)
        logger.info("Webhook по платежу %s отправлен на %s", payment_id, webhook_url)
