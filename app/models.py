import uuid
from datetime import datetime
from enum import auto

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base
from app.enums import LowerStrEnum


class DocumentStatus(LowerStrEnum):
    PROCESSING = auto()
    READY = auto()
    FAILED = auto()


class StepName(LowerStrEnum):
    OCR = auto()
    METADATA = auto()
    CHUNKING = auto()
    EXTERNAL_CALL = auto()


class StepStatus(LowerStrEnum):
    PENDING = auto()              # created, not yet queued
    QUEUED = auto()              # published to the broker
    RUNNING = auto()            # a worker is executing the step
    AWAITING_CALLBACK = auto()  # external_call sent, waiting on the webhook
    DONE = auto()
    ERROR = auto()


def _uuid() -> uuid.UUID:
    return uuid.uuid4()


class Organization(Base):
    __tablename__ = "organization"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)


class AppUser(Base):
    __tablename__ = "app_user"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_uuid)
    org_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("organization.id"), nullable=False, index=True
    )
    email: Mapped[str] = mapped_column(String, unique=True, nullable=False)

    organization: Mapped[Organization] = relationship()


class Document(Base):
    __tablename__ = "document"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_uuid)
    # org_id is denormalised onto every tenant row so every query can filter on it
    # cheaply and safely (defence in depth for isolation).
    org_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("organization.id"), nullable=False
    )
    uploaded_by: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("app_user.id"), nullable=False
    )
    filename: Mapped[str] = mapped_column(String, nullable=False)
    storage_uri: Mapped[str] = mapped_column(String, nullable=False)
    # Derived from the steps (single source of truth) but persisted so the list
    # view does not aggregate child rows on every call.
    status: Mapped[str] = mapped_column(
        String, nullable=False, default=DocumentStatus.PROCESSING.value
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    uploader: Mapped[AppUser] = relationship()
    steps: Mapped[list["PipelineStep"]] = relationship(
        back_populates="document", cascade="all, delete-orphan", order_by="PipelineStep.name"
    )

    __table_args__ = (Index("ix_document_org_created", "org_id", "created_at"),)


class PipelineStep(Base):
    __tablename__ = "pipeline_step"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_uuid)
    document_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("document.id"), nullable=False
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(
        String, nullable=False, default=StepStatus.PENDING.value
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    result: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    error_text: Mapped[str | None] = mapped_column(String, nullable=True)
    external_job_id: Mapped[str | None] = mapped_column(String, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    document: Mapped[Document] = relationship(back_populates="steps")

    __table_args__ = (
        UniqueConstraint("document_id", "name", name="uq_step_document_name"),
        Index("ix_step_external_job_id", "external_job_id"),
        # Partial index: the reaper only ever scans in-flight steps, so this stays
        # tiny no matter how large the history grows.
        Index(
            "ix_step_inflight",
            "status",
            "updated_at",
            postgresql_where=text(
                "status IN ('queued', 'running', 'awaiting_callback')"
            ),
        ),
    )


class WebhookEvent(Base):
    """Append-only audit + idempotency guard for inbound partner webhooks."""

    __tablename__ = "webhook_event"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_uuid)
    job_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    signature_ok: Mapped[bool] = mapped_column(nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
