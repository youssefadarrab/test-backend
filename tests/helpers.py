from __future__ import annotations

import uuid

from app.models import Document, PipelineStep, StepName, StepStatus


def create_document(
    db,
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    *,
    ocr=StepStatus.DONE,
    metadata=StepStatus.DONE,
    chunking=StepStatus.DONE,
    external_call=StepStatus.PENDING,
    external_job_id: str | None = None,
    external_attempts: int = 0,
    statuses_updated_at=None,
) -> uuid.UUID:
    """Create a document with its four steps in the requested states."""
    with db.session_scope() as s:
        doc = Document(
            org_id=org_id,
            uploaded_by=user_id,
            filename="f.pdf",
            storage_uri="x",
            status="processing",
        )
        s.add(doc)
        s.flush()
        wanted = {
            StepName.OCR: ocr,
            StepName.METADATA: metadata,
            StepName.CHUNKING: chunking,
            StepName.EXTERNAL_CALL: external_call,
        }
        for name, st in wanted.items():
            step = PipelineStep(
                document_id=doc.id,
                name=name.value,
                status=st.value,
                result={"text": "x"} if name is StepName.OCR else None,
            )
            if name is StepName.EXTERNAL_CALL:
                step.external_job_id = external_job_id
                step.attempts = external_attempts
            if statuses_updated_at is not None:
                step.updated_at = statuses_updated_at
            s.add(step)
        s.commit()
        return doc.id
