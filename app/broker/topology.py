"""Топология RabbitMQ: единое описание exchanges и очередей для API и consumer.

payments (direct) --payments.new--> [payments.new] --(reject)--> payments.dlx --> [payments.new.dlq]
                                         |
     consumer при ошибке публикует в payments.retry --payments.new.retry.N--> [payments.new.retry.N]
     очередь задержки держит сообщение TTL и по истечении возвращает его в payments / payments.new
"""
from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.core.config import get_settings

PAYMENT_CREATED_EVENT = "payment.created"
PAYMENTS_NEW_ROUTING_KEY = "payments.new"
PAYMENTS_DLQ_ROUTING_KEY = "payments.new.dlq"

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
PAYMENTS_DLX = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)
PAYMENTS_RETRY_EXCHANGE = RabbitExchange("payments.retry", type=ExchangeType.DIRECT, durable=True)

PAYMENTS_NEW_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key=PAYMENTS_NEW_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": PAYMENTS_DLX.name,
        "x-dead-letter-routing-key": PAYMENTS_DLQ_ROUTING_KEY,
    },
)

PAYMENTS_DLQ = RabbitQueue("payments.new.dlq", durable=True, routing_key=PAYMENTS_DLQ_ROUTING_KEY)


def retry_routing_key(attempt: int) -> str:
    return f"payments.new.retry.{attempt}"


def retry_delay_ms(attempt: int) -> int:
    # Экспоненциальная задержка: base, base*2, base*4, ...
    return get_settings().retry_base_delay_seconds * 2 ** (attempt - 1) * 1000


def _build_retry_queues() -> dict[int, RabbitQueue]:
    # Отдельная очередь на каждую попытку: TTL задан на очередь, поэтому сообщения
    # с разной задержкой не блокируют друг друга (в отличие от per-message expiration)
    return {
        attempt: RabbitQueue(
            f"payments.new.retry.{attempt}",
            durable=True,
            routing_key=retry_routing_key(attempt),
            arguments={
                "x-message-ttl": retry_delay_ms(attempt),
                "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
                "x-dead-letter-routing-key": PAYMENTS_NEW_ROUTING_KEY,
            },
        )
        for attempt in range(1, get_settings().max_retries + 1)
    }


PAYMENTS_RETRY_QUEUES = _build_retry_queues()


async def declare_topology(broker: RabbitBroker) -> None:
    """Идемпотентно объявляет exchanges, очереди и их привязки."""
    bindings = [
        (PAYMENTS_EXCHANGE, PAYMENTS_NEW_QUEUE),
        (PAYMENTS_DLX, PAYMENTS_DLQ),
        *((PAYMENTS_RETRY_EXCHANGE, queue) for queue in PAYMENTS_RETRY_QUEUES.values()),
    ]
    for exchange, queue in bindings:
        declared_exchange = await broker.declare_exchange(exchange)
        declared_queue = await broker.declare_queue(queue)
        await declared_queue.bind(declared_exchange, routing_key=queue.routing())
