from faststream.rabbit import Channel, RabbitBroker

from app.core.config import get_settings

broker = RabbitBroker(
    get_settings().rabbit_url,
    default_channel=Channel(
        prefetch_count=get_settings().consumer_prefetch_count,
        # Ждём подтверждения брокера, а сообщение без подходящей очереди считаем ошибкой
        publisher_confirms=True,
        on_return_raises=True,
    ),
)
