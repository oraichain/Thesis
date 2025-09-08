"""
Publisher module for OpenHands worker system.

This module provides different publisher implementations for message queuing:
- BasePublisher: Abstract base class defining the publisher interface
- RedisPublisher: Redis Streams-based publisher implementation
- KafkaPublisher: Kafka topics-based publisher implementation
- create_publisher_from_config: Factory function to create publishers based on configuration

Usage Example:
    from openhands.worker.publisher import RedisPublisher, create_publisher_from_config

    # Create publisher directly
    publisher = RedisPublisher(
        publisher_name="worker_1",
        redis_host="localhost",
        redis_port=6379
    )
    publisher.start()
    publisher.publish("key1", {"data": "value"})  # Publish a message

    # Create publisher from configuration (recommended)
    publisher = create_publisher_from_config(
        publisher_name="worker_1",
        config=my_app_config
    )
    publisher.start()
    publisher.publish("key1", {"data": "value"})  # Publish a message
"""

from openhands.core.config.app_config import AppConfig
from openhands.core.config.worker_config import QueueType

from .base import BasePublisher

# Import publishers with optional dependencies
try:
    from .redis_publisher import RedisPublisher, create_redis_publisher  # noqa

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


def create_publisher_from_config(
    publisher_name: str,
    config: AppConfig,
) -> BasePublisher:
    """
    Create a publisher instance based on the worker configuration.

    Args:
        publisher_name: Unique name for this publisher instance
        config: Application configuration containing worker settings

    Returns:
        BasePublisher: Configured publisher instance based on queue_type configuration

    Raises:
        ValueError: If the queue_type is not supported or not configured
    """
    if not config.worker.queue_type:
        raise ValueError('queue_type is not configured in worker settings')

    if config.worker.queue_type == QueueType.REDIS:
        return create_redis_publisher(
            publisher_name=publisher_name,
            redis_host=config.worker.queue_host,
            redis_port=config.worker.queue_port,
            redis_db=config.worker.queue_db or 0,
            redis_password=config.worker.queue_password,
            num_partitions=config.worker.queue_num_partitions or 4,
            max_messages_per_partitions=config.worker.queue_max_messages_per_partitions
            or 1000,
        )
    else:
        raise ValueError(f'Unsupported queue type: {config.worker.queue_type}')


__all__ = ['BasePublisher', 'create_publisher_from_config']

if REDIS_AVAILABLE:
    __all__.extend(['RedisPublisher', 'create_redis_publisher'])
