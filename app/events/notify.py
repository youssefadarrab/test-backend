"""Realtime progress plumbing.

Write side: workers call `emit_event` inside the same transaction as the status
change, so the NOTIFY fires on commit and can never announce a state that rolled
back.

Read side: each API process runs ONE background listener thread on a dedicated
psycopg2 connection (LISTEN doc_events) and fans out to per-client asyncio
queues. So Postgres connections scale with processes, not with SSE clients.
"""
from __future__ import annotations

import asyncio
import json
import logging
import select
import threading
from collections import defaultdict

import psycopg2
from sqlalchemy import text
from sqlalchemy.orm import Session

CHANNEL = "doc_events"
LOGGER = logging.getLogger("app.events")


def emit_event(session: Session, document_id, event: dict) -> None:
    """Queue a NOTIFY on the current transaction. Payload must stay < 8000 bytes,
    so we send a thin event; clients re-fetch detail when they need it."""
    payload = json.dumps({"document_id": str(document_id), **event})
    session.execute(text("SELECT pg_notify(:chan, :payload)"), {"chan": CHANNEL, "payload": payload})


class EventBroker:
    """In-process pub/sub bridging the LISTEN thread and async SSE handlers."""

    def __init__(self) -> None:
        self._subscribers: dict[str, set[asyncio.Queue]] = defaultdict(set)
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    # ---- subscription (async side) ----
    def subscribe(self, document_id: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        with self._lock:
            self._subscribers[document_id].add(q)
        return q

    def unsubscribe(self, document_id: str, q: asyncio.Queue) -> None:
        with self._lock:
            subs = self._subscribers.get(document_id)
            if subs:
                subs.discard(q)
                if not subs:
                    self._subscribers.pop(document_id, None)

    # ---- dispatch (listener-thread side) ----
    def _dispatch(self, document_id: str, event: dict) -> None:
        if self._loop is None:
            return
        with self._lock:
            queues = list(self._subscribers.get(document_id, ()))
        for q in queues:
            # Hop back onto the event loop thread to touch the asyncio.Queue safely.
            self._loop.call_soon_threadsafe(q.put_nowait, event)

    # ---- lifecycle ----
    def start(self, loop: asyncio.AbstractEventLoop, dsn: str) -> None:
        self._loop = loop
        self._thread = threading.Thread(target=self._run, args=(dsn,), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self, dsn: str) -> None:
        try:
            conn = psycopg2.connect(dsn)
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            cur.execute(f"LISTEN {CHANNEL};")
            LOGGER.info("notify listener started on channel %s", CHANNEL)
        except Exception:  # pragma: no cover - depends on live DB
            LOGGER.exception("notify listener failed to start")
            return

        while not self._stop.is_set():
            if select.select([conn], [], [], 1.0) == ([], [], []):
                continue
            conn.poll()
            while conn.notifies:
                n = conn.notifies.pop(0)
                try:
                    event = json.loads(n.payload)
                    self._dispatch(event["document_id"], event)
                except Exception:  # pragma: no cover
                    LOGGER.exception("bad notify payload: %s", n.payload)


# Single shared broker per process.
broker = EventBroker()
