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

# Per-client buffer cap. A document emits only a handful of deltas, so a client this
# far behind is wedged (dead connection, paused client). We drop deltas rather than
# grow without bound: the snapshot sent on (re)connect is authoritative, so a dropped
# delta is recovered on resync.
_SUBSCRIBER_QUEUE_MAXSIZE = 100

# SSE / NOTIFY payload keys, defined once so producers and consumers agree.
KEY_DOCUMENT_ID = "document_id"
KEY_STEP = "step"
KEY_STATUS = "status"
KEY_DOC_STATUS = "doc_status"
KEY_TYPE = "type"
KEY_STEPS = "steps"
TYPE_SNAPSHOT = "snapshot"


def step_event(step: str, status: str) -> dict:
    return {KEY_STEP: step, KEY_STATUS: status}


def doc_status_event(status: str) -> dict:
    return {KEY_DOC_STATUS: status}


def snapshot_event(document_id: str, doc_status: str, steps: dict[str, str]) -> dict:
    return {KEY_TYPE: TYPE_SNAPSHOT, KEY_DOCUMENT_ID: document_id, KEY_DOC_STATUS: doc_status, KEY_STEPS: steps}


def emit_event(session: Session, document_id, event: dict) -> None:
    """Queue a NOTIFY on the current transaction. Payload must stay < 8000 bytes,
    so we send a thin event; clients re-fetch detail when they need it."""
    payload = json.dumps({KEY_DOCUMENT_ID: str(document_id), **event})
    session.execute(text("SELECT pg_notify(:chan, :payload)"), {"chan": CHANNEL, "payload": payload})


class EventBroker:
    """In-process pub/sub bridging the LISTEN thread and async SSE handlers."""

    def __init__(self) -> None:
        self._subscribers: dict[str, set[asyncio.Queue]] = defaultdict(set)
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self.dropped_events = 0  # deltas dropped to slow consumers (observability)

    # ---- subscription (async side) ----
    def subscribe(self, document_id: str) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=_SUBSCRIBER_QUEUE_MAXSIZE)
        with self._lock:
            self._subscribers[document_id].add(queue)
        return queue

    def unsubscribe(self, document_id: str, queue: asyncio.Queue) -> None:
        with self._lock:
            subscribers = self._subscribers.get(document_id)
            if subscribers:
                subscribers.discard(queue)
                if not subscribers:
                    self._subscribers.pop(document_id, None)

    # ---- dispatch (listener-thread side) ----
    def _dispatch(self, document_id: str, event: dict) -> None:
        if self._loop is None:
            return
        with self._lock:
            queues = list(self._subscribers.get(document_id, ()))
        for queue in queues:
            # Hop back onto the event loop thread to touch the asyncio.Queue safely.
            self._loop.call_soon_threadsafe(self._enqueue, queue, event)

    def _enqueue(self, queue: asyncio.Queue, event: dict) -> None:
        """Runs on the loop thread. Drops the delta if the client's buffer is full,
        rather than letting one slow consumer grow without bound."""
        try:
            queue.put_nowait(event)
        except asyncio.QueueFull:
            self.dropped_events += 1
            LOGGER.warning("dropping event for a slow SSE consumer (queue full)")

    # ---- lifecycle ----
    def start(self, loop: asyncio.AbstractEventLoop, dsn: str) -> None:
        self._loop = loop
        self._thread = threading.Thread(target=self._run, args=(dsn,), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self, dsn: str) -> None:
        try:
            connection = psycopg2.connect(dsn)
            connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = connection.cursor()
            cursor.execute(f"LISTEN {CHANNEL};")
            LOGGER.info("notify listener started on channel %s", CHANNEL)
        except Exception:  # pragma: no cover - depends on live DB
            LOGGER.exception("notify listener failed to start")
            return

        while not self._stop.is_set():
            if select.select([connection], [], [], 1.0) == ([], [], []):
                continue
            connection.poll()
            while connection.notifies:
                notification = connection.notifies.pop(0)
                try:
                    event = json.loads(notification.payload)
                    self._dispatch(event[KEY_DOCUMENT_ID], event)
                except Exception:  # pragma: no cover
                    LOGGER.exception("bad notify payload: %s", notification.payload)


# Single shared broker per process.
broker = EventBroker()
