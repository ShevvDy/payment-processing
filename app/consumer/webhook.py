import httpx

from app.core.config import get_settings
from app.schemas.payment import PaymentWebhook


class WebhookSender:
    def __init__(self) -> None:
        self._client = httpx.AsyncClient(timeout=get_settings().webhook_timeout_seconds)

    async def send(self, url: str, payload: PaymentWebhook) -> None:
        """Отправляет уведомление. Любой ответ кроме 2xx считается ошибкой."""
        response = await self._client.post(url, json=payload.model_dump(mode="json"))
        response.raise_for_status()

    async def close(self) -> None:
        await self._client.aclose()
