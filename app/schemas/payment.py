import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, field_validator

from app.db.models import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: Optional[str] = Field(default=None, max_length=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: AnyHttpUrl

    @field_validator("amount")
    @classmethod
    def normalize_amount(cls, value: Decimal) -> Decimal:
        # 10.5 и 10.50 - один и тот же платёж, приводим к единому виду для хеша идемпотентности
        return value.quantize(Decimal("0.01"))


class PaymentCreatedResponse(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class PaymentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: Optional[str]
    # в ORM-модели атрибут называется metadata_, наружу отдаём metadata
    metadata: dict[str, Any] = Field(validation_alias="metadata_")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: Optional[datetime]


class PaymentCreatedEvent(BaseModel):
    """Тело сообщения в очереди payments.new"""

    payment_id: uuid.UUID


class PaymentWebhook(BaseModel):
    """Тело webhook-уведомления о результате обработки платежа"""

    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(validation_alias="id")
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    metadata: dict[str, Any] = Field(validation_alias="metadata_")
    processed_at: Optional[datetime]
