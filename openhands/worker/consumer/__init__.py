"""
OpenHands Worker Consumer Package.

This package provides abstract base classes and concrete implementations
for different types of message consumers used in the OpenHands worker system.

Available Consumers:
- BaseConsumer: Abstract base class defining the consumer interface
- RedisConsumer: Redis Streams-based consumer implementation
- create_consumer_from_config: Factory function to create consumers based on configuration

Note: ConversationConsumer has been moved to openhands.server.conversation_consumer

Usage Example:
    from openhands.worker.consumer import RedisConsumer, create_consumer_from_config

    # Create consumer directly
    consumer = RedisConsumer(
        consumer_name="worker_1",
        group_name="conversation_group",
        redis_host="localhost",
        redis_port=6379
    )
    consumer.run()  # Start consuming messages

    # Create consumer from configuration (recommended)
    consumer = create_consumer_from_config(
        consumer_name="worker_1",
        group_name="conversation_group",
        message_processor=my_processor
    )
    consumer.run()  # Start consuming messages
"""

from typing import Any, Callable, Optional

from openhands.core.config.app_config import AppConfig
from openhands.core.config.worker_config import QueueType

from .base import BaseConsumer
from .redis_consumer import RedisConsumer, create_redis_consumer


def create_consumer_from_config(
    consumer_name: str,
    group_name: str,
    config: AppConfig,
    message_processor: Optional[Callable[[str, str, dict[str, Any]], None]] = None,
    read_from_start: bool = False,
) -> BaseConsumer:
    """
    Create a consumer instance based on the worker configuration.

    Args:
        consumer_name: Unique name for this consumer instance
        group_name: Name of the consumer group
        message_processor: Optional callback function for processing messages

    Returns:
        BaseConsumer: Configured consumer instance based on queue_type configuration

    Raises:
        ValueError: If the queue_type is not supported or not configured
    """
    if not config.worker.queue_type:
        raise ValueError('queue_type is not configured in worker settings')

    if config.worker.queue_type == QueueType.REDIS:
        return create_redis_consumer(
            consumer_name=consumer_name,
            group_name=group_name,
            redis_host=config.worker.queue_host,
            redis_port=config.worker.queue_port,
            redis_db=config.worker.queue_db or 0,
            redis_password=config.worker.queue_password,
            num_partitions=config.worker.queue_num_partitions or 4,
            message_processor=message_processor,
            read_from_start=read_from_start,
        )
    else:
        raise ValueError(f'Unsupported queue type: {config.worker.queue_type}')


__all__ = [
    'BaseConsumer',
    'RedisConsumer',
    'create_redis_consumer',
    'create_consumer_from_config',
]

__version__ = '1.0.0'
