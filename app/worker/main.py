"""Worker process: consume each per-step queue and run its handler.

prefetch=1 + manual ack AFTER the DB commit is what makes worker crashes safe:
an unacked message is redelivered by RabbitMQ, and the handler is idempotent.
"""
from __future__ import annotations

import json
import logging
import time

from app.db import session_scope
from app.models import StepName
from app.pipeline.handlers import handle_step
from app.worker.broker import connect, declare_topology, queue_name

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("app.worker.main")


def _make_callback(step_value: str):
    def callback(channel, method, _properties, body) -> None:
        action = "retry"
        try:
            payload = json.loads(body)
            with session_scope() as session:
                try:
                    action = handle_step(session, step_value, payload)
                except Exception:
                    session.rollback()
                    LOGGER.exception("handler crashed for step %s", step_value)
                    action = "retry"
        except Exception:
            LOGGER.exception("bad message on %s: %r", step_value, body)
            action = "ack"  # unparseable: drop it, don't poison the queue

        if action == "ack":
            channel.basic_ack(delivery_tag=method.delivery_tag)
        else:
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    return callback


def main() -> None:
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
            LOGGER.exception("worker connection lost; reconnecting in 3s")
            time.sleep(3)


if __name__ == "__main__":
    main()
