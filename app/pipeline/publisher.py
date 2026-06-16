"""Publish a step to the broker.

A short-lived connection per publish keeps the API stateless and avoids sharing a
(non-thread-safe) pika channel across request threads. At the target volume this
is fine; a pooled publisher channel is the obvious optimisation under heavier
load.
"""
from __future__ import annotations

import uuid

import pika

from app.models import StepName
from app.pipeline.messages import BackendStepPayload
from app.worker.broker import connect, declare_topology, queue_name


def publish_step(document_id: uuid.UUID, step: StepName) -> None:
    conn = connect()
    try:
        channel = conn.channel()
        declare_topology(channel)
        channel.basic_publish(
            exchange="",
            routing_key=queue_name(step),
            body=BackendStepPayload(document_id=document_id, step=step).model_dump_json(),
            properties=pika.BasicProperties(
                delivery_mode=2,  # persistent
                content_type="application/json",
            ),
        )
    finally:
        conn.close()
