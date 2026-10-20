import logging
from typing import Any

from faststream import AckPolicy
from faststream.rabbit import RabbitMessage
from pydantic import ValidationError

from app.broker.broker import broker
from app.broker.topology import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    PAYMENTS_RETRY_EXCHANGE,
    retry_routing_key,
)
from app.consumer.gateway import PaymentGateway
from app.consumer.webhook import WebhookSender
from app.core.config import get_settings
from app.schemas.payment import PaymentCreatedEvent
from app.services.processing import PaymentNotFoundError, PaymentProcessor

logger = logging.getLogger(__name__)

RETRY_COUNT_HEADER = "x-retry-count"

webhook_sender = WebhookSender()
processor = PaymentProcessor(PaymentGateway(), webhook_sender)


@broker.subscriber(PAYMENTS_NEW_QUEUE, PAYMENTS_EXCHANGE, ack_policy=AckPolicy.MANUAL)
async def handle_payment_created(body: Any, message: RabbitMessage) -> None:
    try:
        event = PaymentCreatedEvent.model_validate(body)
        await processor.process(event.payment_id)
    except ValidationError:
        logger.error("Невалидное сообщение %s, отправляем в DLQ: %r", message.message_id, body)
        await message.reject(requeue=False)
        return
    except PaymentNotFoundError:
        logger.error("Платёж %s не найден, отправляем в DLQ", event.payment_id)
        await message.reject(requeue=False)
    except Exception as exc:
        await _retry_or_dead_letter(message, event, exc)
    else:
        await message.ack()


async def _retry_or_dead_letter(message: RabbitMessage, event: PaymentCreatedEvent, exc: Exception) -> None:
    retry_count = int(message.headers.get(RETRY_COUNT_HEADER, 0))
    if retry_count >= get_settings().max_retries:
        logger.error(
            "Платёж %s: попытки исчерпаны (%s), отправляем в DLQ: %r",
            event.payment_id, retry_count, exc,
        )
        await message.reject(requeue=False)
        return

    attempt = retry_count + 1
    logger.warning("Платёж %s: ошибка обработки, повтор %s: %r", event.payment_id, attempt, exc)
    try:
        # Сначала публикуем копию в очередь задержки, потом подтверждаем оригинал:
        # при сбое между шагами получим дубль, но не потеряем сообщение
        await broker.publish(
            event.model_dump(mode="json"),
            exchange=PAYMENTS_RETRY_EXCHANGE,
            routing_key=retry_routing_key(attempt),
            persist=True,
            message_id=message.message_id,
            headers={RETRY_COUNT_HEADER: attempt},
        )
    except Exception:
        logger.exception("Не удалось опубликовать повтор для платежа %s, возвращаем в очередь", event.payment_id)
        await message.nack(requeue=True)
        return
    await message.ack()
