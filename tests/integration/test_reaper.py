from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.models import Document, DocumentStatus, PipelineStep, StepName, StepStatus
from app.worker import reaper
from tests.helpers import create_document

OLD = datetime(2020, 1, 1, tzinfo=timezone.utc)


def _ext_step(db, doc_id):
    with db.session_scope() as s:
        return s.execute(
            select(PipelineStep).where(
                PipelineStep.document_id == doc_id,
                PipelineStep.name == StepName.EXTERNAL_CALL.value,
            )
        ).scalar_one()


def test_stale_queued_step_is_republished(db, seeded, fake_publish):
    # external_call queued long ago, attempts left -> reaper re-publishes it.
    doc_id = create_document(
        db, seeded["acme"], seeded["alice"],
        external_call=StepStatus.QUEUED, statuses_updated_at=OLD,
    )
    with db.session_scope() as s:
        reaper.run_once(s)

    assert any(c[1] is StepName.EXTERNAL_CALL for c in fake_publish.calls)


def test_exhausted_stale_step_is_failed(db, seeded, fake_publish, monkeypatch):
    monkeypatch.setattr(reaper.settings, "step_max_attempts", 1)
    # attempts already at the limit, set at creation so updated_at stays OLD.
    doc_id = create_document(
        db, seeded["acme"], seeded["alice"],
        external_call=StepStatus.QUEUED, external_attempts=1, statuses_updated_at=OLD,
    )
    with db.session_scope() as s:
        reaper.run_once(s)

    with db.session_scope() as s:
        assert _ext_step(db, doc_id).status == StepStatus.ERROR.value
        assert s.get(Document, doc_id).status == DocumentStatus.FAILED.value


def test_partner_callback_timeout(db, seeded):
    doc_id = create_document(
        db, seeded["acme"], seeded["alice"],
        external_call=StepStatus.AWAITING_CALLBACK,
        external_job_id="j_ghost",
        statuses_updated_at=OLD,
    )
    with db.session_scope() as s:
        reaper.run_once(s)

    with db.session_scope() as s:
        ext = _ext_step(db, doc_id)
        assert ext.status == StepStatus.ERROR.value
        assert "timeout" in (ext.error_text or "")
        assert s.get(Document, doc_id).status == DocumentStatus.FAILED.value
