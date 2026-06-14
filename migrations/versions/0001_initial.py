"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-06-14
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "organization",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(), nullable=False, unique=True),
    )

    op.create_table(
        "app_user",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("org_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("organization.id"), nullable=False),
        sa.Column("email", sa.String(), nullable=False, unique=True),
    )
    op.create_index("ix_app_user_org_id", "app_user", ["org_id"])

    op.create_table(
        "document",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("org_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("organization.id"), nullable=False),
        sa.Column("uploaded_by", postgresql.UUID(as_uuid=True), sa.ForeignKey("app_user.id"), nullable=False),
        sa.Column("filename", sa.String(), nullable=False),
        sa.Column("storage_uri", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_document_org_created", "document", ["org_id", "created_at"])

    op.create_table(
        "pipeline_step",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("document_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("document.id"), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("result", postgresql.JSONB(), nullable=True),
        sa.Column("error_text", sa.String(), nullable=True),
        sa.Column("external_job_id", sa.String(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.UniqueConstraint("document_id", "name", name="uq_step_document_name"),
    )
    op.create_index("ix_step_external_job_id", "pipeline_step", ["external_job_id"])
    op.create_index(
        "ix_step_inflight",
        "pipeline_step",
        ["status", "updated_at"],
        postgresql_where=sa.text("status IN ('queued', 'running', 'awaiting_callback')"),
    )

    op.create_table(
        "webhook_event",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", sa.String(), nullable=False),
        sa.Column("signature_ok", sa.Boolean(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_webhook_event_job_id", "webhook_event", ["job_id"])


def downgrade() -> None:
    op.drop_table("webhook_event")
    op.drop_index("ix_step_inflight", table_name="pipeline_step")
    op.drop_index("ix_step_external_job_id", table_name="pipeline_step")
    op.drop_table("pipeline_step")
    op.drop_index("ix_document_org_created", table_name="document")
    op.drop_table("document")
    op.drop_index("ix_app_user_org_id", table_name="app_user")
    op.drop_table("app_user")
    op.drop_table("organization")
