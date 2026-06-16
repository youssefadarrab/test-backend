"""Worker process: consume each per-step queue and run its handler.

prefetch=1 + manual ack AFTER the DB commit is what makes worker crashes safe:
an unacked message is redelivered by RabbitMQ, and the handler is idempotent.
"""
from __future__ import annotations

import logging
import time
import uuid

from app.db import session_scope
from app.models import StepName
from app.observability import configure_logging, set_trace_id
from app.pipeline.handlers import handle_step
from app.pipeline.messages import BackendStepPayload
from app.worker.broker import connect, declare_topology, queue_name

LOGGER = logging.getLogger("app.worker")


def _make_callback(step_value: str):
    def callback(channel, method, _properties, body) -> None:
        set_trace_id(uuid.uuid4().hex)
        action = "retry"
        try:
            payload = BackendStepPayload.model_validate_json(body)
            with session_scope() as session:
                try:
                    action = handle_step(session, payload)
                except Exception:
                    session.rollback()
                    LOGGER.exception(
                        "handler crashed",
                        extra={"document_id": str(payload.document_id), "step": payload.step.value},
                    )
                    action = "retry"
        except Exception:
            LOGGER.exception("unparseable message dropped", extra={"queue": step_value})
            action = "ack"  # unparseable: drop it, don't poison the queue

        if action == "ack":
            channel.basic_ack(delivery_tag=method.delivery_tag)
        else:
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    return callback


def main() -> None:
    configure_logging()
    while True:
        try:
            conn = connect()
            channel = conn.channel()
            declare_topology(channel)
            channel.basic_qos(prefetch_count=1)
            for step in StepName:
                channel.basic_consume(queue_name(step), _make_callback(step.value))
            LOGGER.info("worker waiting for messages")
            channel.start_consuming()
        except Exception:
            LOGGER.exception("broker connection lost, reconnecting", extra={"retry_seconds": 3})
            time.sleep(3)


if __name__ == "__main__":
    main()
