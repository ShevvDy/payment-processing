import asyncio
import random

from app.core.config import get_settings
from app.db.models import PaymentStatus


class PaymentGateway:
    """Мок внешнего платёжного шлюза"""

    def __init__(self) -> None:
        self._settings = get_settings()

    async def charge(self) -> PaymentStatus:
        await asyncio.sleep(
            random.uniform(
                self._settings.gateway_min_delay_seconds,
                self._settings.gateway_max_delay_seconds,
            )
        )
        if random.random() < self._settings.gateway_success_rate:
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED
