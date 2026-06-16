"""Stale-task reaper.

Covers two failure modes nothing else does:
  * a step stuck QUEUED because a publish was lost (commit succeeded, publish
    didn't) -> re-publish, or fail it if attempts are exhausted;
  * external_call stuck AWAITING_CALLBACK because the partner never sent the
    webhook -> time it out.

RUNNING steps are left to RabbitMQ redelivery (an unacked message from a dead
worker comes back on its own). The reaper never re-publishes a RUNNING step,
which would risk a second external_call side effect.

Rows are claimed with FOR UPDATE SKIP LOCKED, so multiple reaper instances are
safe with no leader election.
"""
from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from app import transactions
from app.config import get_settings
from app.db import session_scope
from app.events.notify import emit_event
from app.models import PipelineStep, StepName, StepStatus
from app.observability import configure_logging, set_trace_id
from app.pipeline.publisher import publish_step
from app.pipeline.transition import Transitioner

settings = get_settings()
LOGGER = logging.getLogger("app.reaper")
_TRANSITIONER = Transitioner()


def _fail(session: Session, step: PipelineStep, reason: str) -> None:
    step.status = StepStatus.ERROR.value
    step.error_text = reason
    step.finished_at = datetime.now(timezone.utc)
    emit_event(session, step.document_id, {"step": step.name, "status": StepStatus.ERROR.value})
    session.commit()
    _TRANSITIONER.recompute_document_status(session, step.document_id)


def run_once(session: Session) -> int:
    now = datetime.now(timezone.utc)
    acted = 0

    # 1. Steps stuck in flight.
    queued_cutoff = now - timedelta(seconds=settings.step_timeout_seconds)
    stale = transactions.lock_stale_steps(
        session, [StepStatus.QUEUED.value, StepStatus.RUNNING.value], queued_cutoff
    )
    for step in stale:
        if step.attempts >= settings.step_max_attempts:
            LOGGER.error(
                "step failed permanently (reaped)",
                extra={"document_id": str(step.document_id), "step": step.name, "attempt": step.attempts},
            )
            _fail(session, step, "exhausted retries (reaped)")
            acted += 1
        elif step.status == StepStatus.QUEUED.value:
            LOGGER.info(
                "re-queuing stale step",
                extra={"document_id": str(step.document_id), "step": step.name},
            )
            publish_step(step.document_id, StepName(step.name))
            acted += 1
        # RUNNING: leave to broker redelivery.
    session.commit()

    # 2. external_call waiting on a webhook that never came.
    callback_cutoff = now - timedelta(seconds=settings.callback_sla_seconds)
    stuck = transactions.lock_stale_steps(
        session, [StepStatus.AWAITING_CALLBACK.value], callback_cutoff
    )
    for step in stuck:
        LOGGER.warning(
            "partner callback timed out",
            extra={"document_id": str(step.document_id), "step": step.name},
        )
        _fail(session, step, "partner callback timeout")
        acted += 1

    return acted


def main() -> None:
    configure_logging()
    LOGGER.info("reaper started", extra={"interval_seconds": settings.reaper_interval_seconds})
    while True:
        set_trace_id(uuid.uuid4().hex)
        try:
            with session_scope() as session:
                run_once(session)
        except Exception:
            LOGGER.exception("reaper iteration failed")
        time.sleep(settings.reaper_interval_seconds)


if __name__ == "__main__":
    main()
