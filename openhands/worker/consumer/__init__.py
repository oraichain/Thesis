"""
OpenHands Worker Consumer Package.

This package provides abstract base classes and concrete implementations
for different types of message consumers used in the OpenHands worker system.

Available Consumers:
- BaseConsumer: Abstract base class defining the consumer interface
- RedisConsumer: Redis Streams-based consumer implementation
- KafkaConsumer: Apache Kafka-based consumer implementation (template)

Usage Example:
    from openhands.worker.consumer import RedisConsumer

    consumer = RedisConsumer(
        consumer_name="worker_1",
        group_name="conversation_group",
        redis_host="localhost",
        redis_port=6379
    )

    consumer.run()  # Start consuming messages
"""

from .base import BaseConsumer
from .redis_consumer import RedisConsumer, create_redis_consumer

__all__ = ['BaseConsumer', 'RedisConsumer', 'create_redis_consumer']

__version__ = '1.0.0'
