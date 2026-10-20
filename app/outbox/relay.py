import asyncio
import logging
from datetime import datetime, timezone

from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.broker.topology import PAYMENTS_EXCHANGE
from app.core.config import get_settings
from app.db.session import session_factory
from app.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)


class OutboxRelay:
    """Фоновая идемпортентная публикация событий из таблицы outbox в RabbitMQ"""

    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker
        self._settings = get_settings()
        self._stopped = asyncio.Event()

    async def run(self) -> None:
        while not self._stopped.is_set():
            try:
                published = await self.publish_batch()
            except Exception:
                logger.exception("Ошибка при публикации событий из outbox")
                published = 0
            # Если опубликовано меньше, чем размер батча, то ждем некоторое время перед следующей попыткой
            if published < self._settings.outbox_batch_size:
                try:
                    await asyncio.wait_for(self._stopped.wait(), self._settings.outbox_poll_interval)
                except asyncio.TimeoutError:
                    pass

    def stop(self) -> None:
        self._stopped.set()

    async def publish_batch(self) -> int:
        async with session_factory() as session, session.begin():
            repository = OutboxRepository(session)
            # FOR UPDATE SKIP LOCKED позволяет запускать несколько реплик без двойной публикации
            events = await repository.lock_unpublished(self._settings.outbox_batch_size)
            published = 0
            for event in events:
                try:
                    await self._broker.publish(
                        event.payload,
                        exchange=PAYMENTS_EXCHANGE,
                        routing_key=event.routing_key,
                        persist=True,
                        message_id=str(event.id),
                        message_type=event.event_type,
                    )
                except Exception as exc:
                    event.attempts += 1
                    event.last_error = repr(exc)
                    logger.warning("Ошибка при публикации события %s: %r", event.id, exc)
                    break
                event.published_at = datetime.now(timezone.utc)
                published += 1
        return published
