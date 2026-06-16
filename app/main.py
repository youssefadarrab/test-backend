from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import auth, dev, documents, events, webhooks
from app.config import get_settings
from app.events.notify import broker

logging.basicConfig(level=logging.INFO)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # per-process LISTEN thread, bound to this loop, feeds SSE fan-out
    broker.start(asyncio.get_running_loop(), settings.listen_dsn)
    try:
        yield
    finally:
        broker.stop()


app = FastAPI(title="Primmo Document Pipeline", version="0.1.0", lifespan=lifespan)

API_V1 = "/v1/docpipe"

app.include_router(auth.router, prefix=API_V1)
app.include_router(documents.router, prefix=API_V1)
app.include_router(events.router, prefix=API_V1)
app.include_router(webhooks.router, prefix=API_V1)

# Local-only signature helper; mounted only when ENV=local.
if settings.is_local:
    app.include_router(dev.router, prefix=API_V1)


# Health stays unversioned: it's for infra purposes, not API clients.
@app.get("/healthz", tags=["meta"])
def healthz() -> dict:
    return {"status": "ok"}
