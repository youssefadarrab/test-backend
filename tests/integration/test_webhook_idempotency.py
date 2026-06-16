from __future__ import annotations

import json

from sqlalchemy import select

from app.models import Document, DocumentStatus, PipelineStep, StepName, StepStatus
from app.webhook_security import compute_signature
from tests.helpers import create_document

JOB_ID = "j_idem123456789"


def _post(client, body: dict, sig: str | None = None):
    raw = json.dumps(body).encode()
    signature = sig if sig is not None else compute_signature(raw)
    return client.post(
        "/webhooks/partner",
        content=raw,
        headers={"Content-Type": "application/json", "X-Partner-Signature": signature},
    )


def test_bad_signature_rejected(client, seeded, db):
    create_document(
        db, seeded["acme"], seeded["alice"],
        external_call=StepStatus.AWAITING_CALLBACK, external_job_id=JOB_ID,
    )
    resp = _post(client, {"job_id": JOB_ID, "status": "completed", "result": {}}, sig="bad")
    assert resp.status_code == 401


def test_webhook_marks_ready_then_is_idempotent(client, seeded, db):
    doc_id = create_document(
        db, seeded["acme"], seeded["alice"],
        external_call=StepStatus.AWAITING_CALLBACK, external_job_id=JOB_ID,
    )
    body = {"job_id": JOB_ID, "status": "completed", "result": {"indexed_at": "2026-05-21T14:23:11Z"}}

    first = _post(client, body)
    assert first.status_code == 200
    assert first.json()["document_status"] == DocumentStatus.READY.value

    # Replay: no further state change.
    second = _post(client, body)
    assert second.status_code == 200
    assert second.json()["status"] == "duplicate-ignored"

    with db.session_scope() as s:
        doc = s.get(Document, doc_id)
        assert doc.status == DocumentStatus.READY.value
        ext = s.execute(
            select(PipelineStep).where(
                PipelineStep.document_id == doc_id,
                PipelineStep.name == StepName.EXTERNAL_CALL.value,
            )
        ).scalar_one()
        assert ext.status == StepStatus.DONE.value


def test_unknown_job_id_is_accepted_opaquely(client, seeded, db):
    resp = _post(client, {"job_id": "j_nope", "status": "completed", "result": {}})
    assert resp.status_code == 200
    assert resp.json()["status"] == "accepted"
