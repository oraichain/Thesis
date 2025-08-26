"""
Publisher module for OpenHands worker system.

This module provides different publisher implementations for message queuing:
- BasePublisher: Abstract base class defining the publisher interface
- RedisPublisher: Redis Streams-based publisher implementation
- KafkaPublisher: Kafka topics-based publisher implementation
"""

from .base import BasePublisher

# Import publishers with optional dependencies
try:
    from .redis_publisher import RedisPublisher  # noqa

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

__all__ = ['BasePublisher']

if REDIS_AVAILABLE:
    __all__.append('RedisPublisher')
