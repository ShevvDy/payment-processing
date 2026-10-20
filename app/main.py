import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.router import api_router
from app.broker.broker import broker
from app.broker.topology import declare_topology
from app.db.session import engine
from app.outbox.relay import OutboxRelay


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    await broker.connect()
    await declare_topology(broker)

    relay = OutboxRelay(broker)
    relay_task = asyncio.create_task(relay.run())
    try:
        yield
    finally:
        relay.stop()
        await relay_task
        await broker.stop()
        await engine.dispose()


app = FastAPI(lifespan=lifespan)
app.include_router(api_router)
