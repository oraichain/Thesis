"""
Redis implementation of the BasePublisher class.

This module provides a Redis-based publisher that implements the abstract methods
from BasePublisher using Redis Streams for message queuing.
"""

import hashlib
from typing import Any, Dict, Optional

import redis
from opentelemetry import trace

from openhands.utils.messaging_tracing import inject_trace_context, start_producer_span

from .base import BasePublisher

trace_provider = trace.get_tracer_provider()
tracer = trace_provider.get_tracer(__name__)


class RedisPublisher(BasePublisher):
    """
    Redis-based implementation of BasePublisher using Redis Streams.

    This publisher uses Redis Streams for message queuing with support for
    partitioning and automatic distribution of messages across multiple streams.
    """

    def __init__(
        self,
        publisher_name: str,
        redis_host: str = 'localhost',
        redis_port: int = 6379,
        redis_db: int = 0,
        redis_password: Optional[str] = None,
        stream_name_template: str = 'conversation_partition_{}',
        num_partitions: int = 4,
        max_messages_per_partitions: int = 1000,
    ):
        """
        Initialize Redis publisher.

        Args:
            publisher_name: Unique name for this publisher instance
            redis_host: Redis server host
            redis_port: Redis server port
            redis_db: Redis database number
            redis_password: Redis password (if required)
            stream_name_template: Template for stream names (must contain {})
            num_partitions: Number of partitions to distribute messages across
            max_messages_per_partitions: Maximum number of messages per partition
        """
        super().__init__(publisher_name=publisher_name)

        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_password = redis_password
        self.stream_name_template = stream_name_template
        self.num_partitions = num_partitions
        self.max_messages_per_partitions = max_messages_per_partitions

        self.redis_client: Optional[redis.Redis] = None

    def get_partition(self, key: str) -> int:
        """
        Hash the key to determine the partition for Redis streams.

        Args:
            key: Message key to hash

        Returns:
            Partition number (0 to num_partitions-1)
        """
        key_hash = int(hashlib.md5(str(key).encode()).hexdigest(), 16)
        return key_hash % self.num_partitions

    def get_stream_name(self, partition: int) -> str:
        """
        Get the stream name for a specific partition.

        Args:
            partition: Partition number

        Returns:
            Stream name for the partition
        """
        return self.stream_name_template.format(partition)

    def connect(self) -> None:
        """Establish connection to Redis."""
        try:
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                password=self.redis_password,
                decode_responses=True,
            )

            # Test the connection
            self.redis_client.ping()
            print(f'Connected to Redis at {self.redis_host}:{self.redis_port}')

        except redis.ConnectionError as e:
            raise ConnectionError(f'Failed to connect to Redis: {e}')
        except Exception as e:
            raise RuntimeError(f'Error connecting to Redis: {e}')

    def disconnect(self) -> None:
        """Close connection to Redis."""
        if self.redis_client:
            try:
                self.redis_client.close()
                print('Disconnected from Redis')
            except Exception as e:
                print(f'Error disconnecting from Redis: {e}')
            finally:
                self.redis_client = None

    def publish_message(self, key: str, data: Dict[str, Any]) -> str:
        """
        Publish a message to Redis Stream.

        Args:
            key: Message key used for partitioning
            data: Message data to publish

        Returns:
            Message ID from Redis Stream
        """
        if not self.redis_client:
            raise RuntimeError('Redis client not connected')

        # Get the partition and stream name
        partition = self.get_partition(key)
        stream_name = self.get_stream_name(partition)

        # Start producer span with tracing utilities
        with start_producer_span(
            'redis-publish-message',
            messaging_system='redis',
            destination=stream_name,
            message_id=key,
            additional_attributes={
                'redis.partition': partition,
                'messaging.destination_kind': 'stream',
            },
        ) as span:
            if self.max_messages_per_partitions:
                if (
                    self.redis_client.xlen(stream_name)
                    >= self.max_messages_per_partitions
                ):
                    self.redis_client.xtrim(
                        stream_name,
                        maxlen=int(self.max_messages_per_partitions / 2),
                        approximate=True,
                    )

            # Prepare the message with partition info for Redis
            message = self.prepare_message(key, data)
            message['partition'] = str(
                partition
            )  # Add partition info for debugging/tracking

            # Inject trace context into the message
            message = inject_trace_context(message)

            try:
                # Add message to the partition-specific stream
                message_id = self.redis_client.xadd(stream_name, message)
                span.set_attribute('messaging.redis.message_id', message_id)
                return message_id

            except Exception as e:
                span.record_exception(e)
                raise RuntimeError(f'Failed to publish message to Redis: {e}')

    def get_stream_info(self, partition: int) -> Dict[str, Any]:
        """
        Get information about a specific partition stream.

        Args:
            partition: Partition number

        Returns:
            Stream information dictionary
        """
        if not self.redis_client:
            raise RuntimeError('Redis client not connected')

        stream_name = self.get_stream_name(partition)

        try:
            return self.redis_client.xinfo_stream(stream_name)
        except redis.ResponseError:
            # Stream doesn't exist yet
            return {'length': 0, 'stream_name': stream_name}
        except Exception as e:
            raise RuntimeError(f'Failed to get stream info: {e}')

    def get_all_streams_info(self) -> Dict[int, Dict[str, Any]]:
        """
        Get information about all partition streams.

        Returns:
            Dictionary mapping partition numbers to stream info
        """
        streams_info = {}

        for partition in range(self.num_partitions):
            streams_info[partition] = self.get_stream_info(partition)

        return streams_info

    def delete_stream(self, partition: int) -> bool:
        """
        Delete a partition stream (for testing/cleanup).

        Args:
            partition: Partition number to delete

        Returns:
            True if successful, False otherwise
        """
        if not self.redis_client:
            raise RuntimeError('Redis client not connected')

        stream_name = self.get_stream_name(partition)

        try:
            result = self.redis_client.delete(stream_name)
            return result > 0
        except Exception as e:
            print(f'Error deleting stream {stream_name}: {e}')
            return False


def create_redis_publisher(
    publisher_name: str,
    redis_host: str = 'localhost',
    redis_port: int = 6379,
    redis_db: int = 0,
    redis_password: Optional[str] = None,
    num_partitions: int = 4,
    **kwargs,
) -> RedisPublisher:
    """
    Factory function to create a RedisPublisher instance.

    Args:
        publisher_name: Unique name for this publisher instance
        redis_host: Redis server host
        redis_port: Redis server port
        redis_db: Redis database number
        redis_password: Redis password (if required)
        num_partitions: Number of partitions to distribute messages across
        **kwargs: Additional arguments passed to RedisPublisher constructor

    Returns:
        Configured RedisPublisher instance
    """
    return RedisPublisher(
        publisher_name=publisher_name,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        redis_password=redis_password,
        num_partitions=num_partitions,
        **kwargs,
    )
