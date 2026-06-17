import asyncio

from app.events.notify import _SUBSCRIBER_QUEUE_MAXSIZE, EventBroker


def test_subscribe_returns_a_bounded_queue():
    broker = EventBroker()
    queue = broker.subscribe("doc-1")
    assert queue.maxsize == _SUBSCRIBER_QUEUE_MAXSIZE


def test_enqueue_drops_instead_of_growing_when_full():
    broker = EventBroker()
    queue: asyncio.Queue = asyncio.Queue(maxsize=2)

    broker._enqueue(queue, {"n": 1})
    broker._enqueue(queue, {"n": 2})
    assert queue.qsize() == 2

    # Buffer full: the delta is dropped, the queue does not grow, drop is counted.
    broker._enqueue(queue, {"n": 3})
    assert queue.qsize() == 2
    assert broker.dropped_events == 1
