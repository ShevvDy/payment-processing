# Микросервис обработки платежей

Микросервис принимает запросы на оплату, обрабатывает их через эмуляцию платёжного шлюза
и уведомляет клиента о результате через webhook.

**Стек:** FastAPI, Pydantic v2, SQLAlchemy 2.0 (async), PostgreSQL, RabbitMQ (FastStream), Alembic, Docker Compose.

## Запуск

```bash
cp .env.example .env
docker compose up --build
```

Поднимаются четыре сервиса:

| Сервис     | Назначение                                                     | Порт                                      |
|------------|----------------------------------------------------------------|-------------------------------------------|
| `postgres` | БД платежей и outbox                                           | 5432                                      |
| `rabbitmq` | Брокер сообщений                                               | 5672, веб-интерфейс - 15672 (guest/guest) |
| `api`      | HTTP API + фоновая публикация событий из outbox                | 8000                                      |
| `consumer` | Обработка платежей, отправка webhook, повторы                  | -                                         |

Миграции применяются автоматически при старте `api` (`alembic upgrade head`).

- Документация API (Swagger): http://localhost:8000/docs
- Веб-интерфейс RabbitMQ: http://localhost:15672

### Переменные окружения

Все переменные описаны в [.env.example](.env.example). Если `.env` отсутствует, используются значения по умолчанию из `docker-compose.yml`.

| Переменная                  | По умолчанию | Описание                                           |
|-----------------------------|--------------|----------------------------------------------------|
| `API_KEY`                   | `change-me`  | Ключ для заголовка `X-API-Key`                     |
| `OUTBOX_POLL_INTERVAL`      | `1.0`        | Период опроса таблицы outbox, сек                  |
| `OUTBOX_BATCH_SIZE`         | `100`        | Сколько событий публикуется за одну итерацию       |
| `CONSUMER_PREFETCH_COUNT`   | `10`         | Сколько сообщений consumer обрабатывает параллельно |
| `MAX_RETRIES`               | `3`          | Число повторов после первой неудачной попытки      |
| `RETRY_BASE_DELAY_SECONDS`  | `2`          | Базовая задержка повтора (2, 4, 8 сек)             |
| `WEBHOOK_TIMEOUT_SECONDS`   | `10`         | Таймаут HTTP-запроса webhook                       |
| `GATEWAY_MIN_DELAY_SECONDS` | `2`          | Минимальное время обработки в шлюзе                |
| `GATEWAY_MAX_DELAY_SECONDS` | `5`          | Максимальное время обработки в шлюзе               |
| `GATEWAY_SUCCESS_RATE`      | `0.9`        | Доля успешных платежей                             |

## API

Все эндпоинты требуют заголовок `X-API-Key`. Без него или с неверным ключом - `401`.

### Создание платежа

`POST /api/v1/payments`, заголовок `Idempotency-Key` обязателен.

Для проверки webhook удобно взять одноразовый адрес на https://webhook.site.

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: supersecretkey" \
  -H "Idempotency-Key: order-67" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Оплата заказа #67",
        "metadata": {"order_id": 67},
        "webhook_url": "https://webhook.site/<your-uuid>"
      }'
```

Ответ `202 Accepted`:

```json
{
  "payment_id": "3f4f23ca-cde8-4849-aea2-cfeccc6ceee0",
  "status": "pending",
  "created_at": "2026-01-26T11:33:11.816220Z"
}
```

Поля тела запроса:

| Поле          | Тип                     | Обязательное | Ограничения                     |
|---------------|-------------------------|--------------|---------------------------------|
| `amount`      | decimal (строка/число)  | да           | `> 0`, до 2 знаков после запятой |
| `currency`    | `RUB` \| `USD` \| `EUR` | да           |                                 |
| `description` | string                  | нет          | до 1000 символов                |
| `metadata`    | object                  | нет          | произвольный JSON               |
| `webhook_url` | URL                     | да           | схема http или https            |

Повторный запрос с тем же `Idempotency-Key` и тем же телом возвращает уже созданный платёж
(новый платёж не создаётся). Тот же ключ с другим телом - `409 Conflict`.

### Получение платежа

```bash
curl http://localhost:8000/api/v1/payments/3f4f23ca-cde8-4849-aea2-cfeccc6ceee0 \
  -H "X-API-Key: supersecretkey"
```

```json
{
  "id": "3f4f23ca-cde8-4849-aea2-cfeccc6ceee0",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Оплата заказа #42",
  "metadata": {"order_id": 42},
  "status": "succeeded",
  "idempotency_key": "order-42",
  "webhook_url": "https://webhook.site/<your-uuid>",
  "created_at": "2026-10-20T11:33:11.816220Z",
  "processed_at": "2026-10-20T11:33:15.402114Z"
}
```

Несуществующий платёж - `404`.

### Webhook

После обработки на `webhook_url` отправляется `POST`:

```json
{
  "payment_id": "3f4f23ca-cde8-4849-aea2-cfeccc6ceee0",
  "status": "succeeded",
  "amount": "1500.00",
  "currency": "RUB",
  "metadata": {"order_id": 42},
  "processed_at": "2026-10-20T11:33:15.402114Z"
}
```

Успешной доставкой считается любой ответ `2xx`. Из-за повторов webhook может прийти
несколько раз - получателю стоит дедуплицировать по `payment_id`.

## Архитектура

### Outbox pattern

- Платёж и событие `payment.created` записываются в таблицы `payments` и `outbox` **в одной транзакции** -
  событие не может потеряться или появиться без платежа.
- Фоновая задача в процессе `api` (relay) раз в `OUTBOX_POLL_INTERVAL` забирает пачку неопубликованных событий через
  `SELECT ... FOR UPDATE SKIP LOCKED`, поэтому `api` можно масштабировать без двойной публикации.
- Событие помечается опубликованным (`published_at`) только после подтверждения от брокера (publisher confirms).
  Сообщение, для которого не нашлось очереди, считается ошибкой.
- При ошибке публикации у события увеличивается `attempts` и сохраняется `last_error`, публикация повторится в следующей итерации.

Гарантия доставки - "как минимум один раз" (at-least-once): при сбое событие может уйти повторно, поэтому обработка в consumer идемпотентна.

### Идемпотентность

- **API:** уникальный `idempotency_key` в таблице `payments` + sha256 нормализованного тела запроса (`request_hash`).
  Гонка двух одновременных запросов с одним ключом разрешается уникальным индексом.
- **Consumer:** статус меняется запросом `UPDATE ... WHERE status = 'pending'`. При повторной доставке
  шлюз повторно не вызывается - повторяется только отправка webhook.

### Обработка, повторы и DLQ

Consumer подписан на `payments.new` и подтверждает сообщения вручную (ack/reject):

1. Эмуляция шлюза: 2–5 сек, 90% `succeeded`, 10% `failed`. Отказ шлюза - это бизнес-результат, а не ошибка, повторов не вызывает.
2. Статус и `processed_at` сохраняются в БД.
3. Отправляется webhook.

Если шаг 2 или 3 завершился ошибкой (недоступна БД, webhook ответил не `2xx`, таймаут):

- пока попыток меньше `MAX_RETRIES`, копия сообщения публикуется в `payments.retry` с заголовком
  `x-retry-count` и попадает в очередь задержки `payments.new.retry.N`
  (2, 4, 8 сек - экспоненциально). По истечении TTL RabbitMQ возвращает её в `payments.new`;
- после исчерпания попыток сообщение отклоняется (`reject` без возврата в очередь) и через `payments.dlx` попадает в `payments.new.dlq`.

Невалидные сообщения и сообщения о несуществующих платежах сразу уходят в DLQ.

На каждую попытку своя очередь с фиксированным TTL: так сообщения с разной задержкой не ждут друг друга.
Параметры очередей задаются при объявлении, поэтому после изменения `MAX_RETRIES` или
`RETRY_BASE_DELAY_SECONDS` очереди `payments.new.retry.*` нужно удалить (через веб-интерфейс RabbitMQ или пересоздав том `rabbitmq_data`).

## Как проверить

**Успешный сценарий.** Создайте платёж с `webhook_url` от webhook.site. Через 2–5 секунд `GET` вернёт
`succeeded` или `failed`, а на webhook.site появится запрос.

**Идемпотентность.** Повторите тот же запрос с тем же `Idempotency-Key` - вернётся тот же `payment_id`.
Измените сумму, оставив ключ - `409`.

**Повторы и DLQ.** Укажите недоступный webhook, например `http://localhost:9/hook`. В логах consumer
(`docker compose logs -f consumer`) будут видны повторы с растущей задержкой, после чего сообщение
окажется в очереди `payments.new.dlq` (веб-интерфейс RabbitMQ -> Queues). Статус платежа при этом уже сохранён.

**Outbox.** Остановите брокер (`docker compose stop rabbitmq`), создайте платёж - API ответит `202`,
событие останется в таблице `outbox` с `published_at IS NULL`. После `docker compose start rabbitmq`
оно будет опубликовано и обработано.

## Структура проекта

```
app/
  api/            # HTTP-роутеры
  broker/         # RabbitBroker и топология exchanges/queues
  consumer/       # FastStream-приложение: подписчик, шлюз, webhook
  core/           # настройки и проверка API-ключа
  db/             # engine, сессии, ORM-модели
  outbox/         # фоновая публикация событий из outbox
  repositories/   # доступ к таблицам
  schemas/        # Pydantic-схемы
  services/       # создание и обработка платежей
  main.py         # FastAPI-приложение
migrations/       # Alembic
```

## Локальный запуск без Docker

Нужны запущенные PostgreSQL и RabbitMQ и Python 3.10.

```bash
python -m venv venv
source venv/bin/activate 
pip install -r requirements.txt
alembic upgrade head
uvicorn app.main:app --reload
faststream run app.consumer.main:app  # в отдельной консоли
```
