"""When metadata and chunking finish concurrently, external_call is triggered
EXACTLY once."""
from __future__ import annotations

import threading

from sqlalchemy import select

from app.models import PipelineStep, StepName, StepStatus
from app.pipeline.transition import Transitioner
from tests.helpers import create_document


def test_concurrent_fan_in_triggers_external_call_exactly_once(db, seeded, fake_publish):
    # metadata + chunking already DONE; external_call still PENDING.
    doc_id = create_document(db, seeded["acme"], seeded["alice"])

    barrier = threading.Barrier(2)

    def finisher():
        # Each finisher uses its own session, like two separate workers.
        with db.session_scope() as s:
            barrier.wait()  # maximise the race
            Transitioner().trigger_successors(s, doc_id)

    t1 = threading.Thread(target=finisher)
    t2 = threading.Thread(target=finisher)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    external_publishes = [c for c in fake_publish.calls if c[1] is StepName.EXTERNAL_CALL]
    assert len(external_publishes) == 1, fake_publish.calls

    with db.session_scope() as s:
        ext = s.execute(
            select(PipelineStep).where(
                PipelineStep.document_id == doc_id,
                PipelineStep.name == StepName.EXTERNAL_CALL.value,
            )
        ).scalar_one()
        assert ext.status == StepStatus.QUEUED.value


def test_fan_in_waits_for_both_predecessors(db, seeded, fake_publish):
    # Only metadata done; chunking still pending -> external_call must NOT fire.
    doc_id = create_document(
        db, seeded["acme"], seeded["alice"], chunking=StepStatus.RUNNING
    )
    with db.session_scope() as s:
        Transitioner().trigger_successors(s, doc_id)

    assert [c for c in fake_publish.calls if c[1] is StepName.EXTERNAL_CALL] == []
