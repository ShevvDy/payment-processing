import logging

from faststream import FastStream

from app.broker.broker import broker
from app.broker.topology import declare_topology
from app.consumer.handlers import webhook_sender
from app.db.session import engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

app = FastStream(broker)


@app.after_startup
async def setup_topology() -> None:
    # Очереди повторов и DLQ подписчик сам не объявляет
    await declare_topology(broker)


@app.after_shutdown
async def close_resources() -> None:
    await webhook_sender.close()
    await engine.dispose()
