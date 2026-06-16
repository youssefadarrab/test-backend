"""SSE progress stream.

This is the one async endpoint; streams are long-lived, so they must not each
occupy a threadpool thread. It subscribes to the in-process broker FIRST, then
reads a snapshot, so no status change between the snapshot and the live stream is
lost. The snapshot is authoritative, so a duplicated delta is harmless.
"""
from __future__ import annotations

import asyncio
import json
import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse

from app.auth import get_current_user
from app.db import session_scope
from app.events.notify import broker, snapshot_event
from app.models import AppUser, Document

router = APIRouter(tags=["events"])

_KEEPALIVE_SECONDS = 15


def _sse(payload: dict) -> str:
    return f"data: {json.dumps(payload)}\n\n"


def _load_snapshot(document_id: uuid.UUID, org_id: uuid.UUID) -> dict | None:
    """Sync DB read (runs in a thread). Returns None if not found / wrong tenant."""
    with session_scope() as db:
        doc = db.get(Document, document_id)
        if doc is None or doc.org_id != org_id:
            return None
        return snapshot_event(str(doc.id), doc.status, {s.name: s.status for s in doc.steps})


@router.get("/documents/{document_id}/events")
async def stream_events(
    document_id: uuid.UUID,
    user: AppUser = Depends(get_current_user),
) -> StreamingResponse:
    loop = asyncio.get_running_loop()

    # Subscribe before snapshot so the gap is buffered, not dropped.
    queue = broker.subscribe(str(document_id))
    snapshot = await loop.run_in_executor(None, _load_snapshot, document_id, user.org_id)
    if snapshot is None:
        broker.unsubscribe(str(document_id), queue)
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="document not found")

    async def generator():
        try:
            yield _sse(snapshot)
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=_KEEPALIVE_SECONDS)
                    yield _sse(event)
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"  # comment frame keeps the connection warm
        finally:
            broker.unsubscribe(str(document_id), queue)

    return StreamingResponse(generator(), media_type="text/event-stream")
