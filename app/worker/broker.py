"""RabbitMQ topology and connection helpers.

The broker is a transient delivery pipe; all durable state lives in Postgres.
Per-step quorum queues carry a dead-letter exchange and a delivery-limit so a
message that keeps failing (or a crashed-worker redelivery loop) is bounded by
the broker as a backstop, while the DB `attempts` counter drives the normal
terminal-failure path.
"""
from __future__ import annotations

import pika

from app.config import get_settings
from app.models import StepName

settings = get_settings()

DLX_EXCHANGE = "dlx"
DEAD_LETTER_QUEUE = "dead_letter"


def queue_name(step: StepName) -> str:
    return f"step.{step.value}"


def connect() -> pika.BlockingConnection:
    return pika.BlockingConnection(pika.URLParameters(settings.rabbitmq_url))


def declare_topology(channel: pika.adapters.blocking_connection.BlockingChannel) -> None:
    """Idempotently declare the DLX and every per-step queue."""
    channel.exchange_declare(exchange=DLX_EXCHANGE, exchange_type="fanout", durable=True)
    channel.queue_declare(
        queue=DEAD_LETTER_QUEUE, durable=True, arguments={"x-queue-type": "quorum"}
    )
    channel.queue_bind(queue=DEAD_LETTER_QUEUE, exchange=DLX_EXCHANGE)

    for step in StepName:
        channel.queue_declare(
            queue=queue_name(step),
            durable=True,
            arguments={
                "x-queue-type": "quorum",
                "x-dead-letter-exchange": DLX_EXCHANGE,
                "x-delivery-limit": settings.step_max_attempts,
            },
        )
