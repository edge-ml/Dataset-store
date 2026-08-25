"""RabbitMQ publisher.

Ports the behaviour of the backend service's messageBroker/publisher.js:
messages are published to a durable queue as JSON of the shape
`{"command": <command>, "payload": <payload>}` so that downstream consumers
(ml service, dataset-store's own MessageQueue.py consumer) keep working.
"""
import json
import logging
import threading

import pika

import internal.config as config

logger = logging.getLogger("dataset-store.mq")

_local = threading.local()


def _get_channel():
    """Return a cached channel per thread, reconnecting when necessary."""
    channel = getattr(_local, "channel", None)
    connection = getattr(_local, "connection", None)
    if channel is not None and connection is not None and connection.is_open:
        return channel
    try:
        if connection is not None and connection.is_open:
            connection.close()
    except Exception:
        pass
    connection = pika.BlockingConnection(pika.URLParameters(config.RABBITMQ_URI))
    channel = connection.channel()
    channel.queue_declare(queue=config.RABBITMQ_QUEUE, durable=True)
    _local.connection = connection
    _local.channel = channel
    return channel


def publish(command: str, payload) -> bool:
    """Publish a command message. Returns True on success, False otherwise."""
    msg = {"command": command, "payload": payload}
    try:
        channel = _get_channel()
        channel.basic_publish(
            exchange="",
            routing_key=config.RABBITMQ_QUEUE,
            body=json.dumps(msg),
            properties=pika.BasicProperties(delivery_mode=2),  # persistent
        )
        return True
    except Exception as exc:  # never let broker hiccups break the request path
        logger.error("Failed to publish %r to RabbitMQ: %s", command, exc)
        # drop the stale handle so the next call reconnects
        _local.channel = None
        _local.connection = None
        return False
