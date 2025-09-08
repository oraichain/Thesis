"""
Redis implementation of the BaseConsumer class.

This module provides a Redis-based consumer that implements the abstract methods
from BaseConsumer using Redis Streams for message queuing.
"""

import json
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import redis
from opentelemetry import trace

from openhands.utils.messaging_tracing import (
    clean_trace_context_from_message,
    start_consumer_span,
)

from .base import BaseConsumer

trace_provider = trace.get_tracer_provider()
tracer = trace_provider.get_tracer(__name__)


class RedisConsumer(BaseConsumer):
    """
    Redis-based implementation of BaseConsumer using Redis Streams.

    This consumer uses Redis Streams for message queuing with support for
    consumer groups, partitioning, and automatic rebalancing.
    """

    def __init__(
        self,
        consumer_name: str,
        group_name: str = '',
        redis_host: str = 'localhost',
        redis_port: int = 6379,
        redis_db: int = 0,
        redis_password: Optional[str] = None,
        stream_name_template: str = 'conversation_partition_{}',
        num_partitions: int = 4,
        heartbeat_interval: int = 5,
        heartbeat_timeout: int = 15,
        rebalance_check_interval: int = 2,
        message_processor: Optional[Callable[[str, str, Dict[str, Any]], None]] = None,
        read_from_start: bool = False,
    ):
        """
        Initialize Redis consumer.

        Args:
            consumer_name: Unique name for this consumer instance
            group_name: Name of the consumer group
            redis_host: Redis server host
            redis_port: Redis server port
            redis_db: Redis database number
            redis_password: Redis password (if required)
            stream_name_template: Template for stream names (must contain {})
            num_partitions: Number of partitions to use
            heartbeat_interval: Seconds between heartbeats
            heartbeat_timeout: Seconds before a consumer is considered inactive
            rebalance_check_interval: Seconds between rebalance checks
            message_processor: Optional callback function for processing messages.
                           If provided, this will be called instead of the default process_message implementation.
                           Signature: (message_id: str, key: str, message_data: Dict[str, Any]) -> None
            read_from_start: If True, consumer will read messages from the beginning of the stream.
                           If False (default), consumer will only read new messages.
        """
        super().__init__(
            consumer_name=consumer_name,
            group_name=group_name,
            num_partitions=num_partitions,
        )

        # Redis-specific configuration
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.rebalance_check_interval = rebalance_check_interval
        self.message_processor = message_processor
        self.read_from_start = read_from_start

        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_password = redis_password
        self.stream_name_template = stream_name_template

        self.redis_client: Optional[redis.Redis] = None

        # Group-specific Redis keys to avoid conflicts with other consumer groups
        self.consumers_key = f'consumers_{self.group_name}'
        self.partition_assignments_key = f'partition_assignments_{self.group_name}'
        self.rebalance_version_key = f'rebalance_version_{self.group_name}'

        # Redis-specific state
        self.heartbeat_thread: Optional[threading.Thread] = None
        self.last_rebalance_version = 0
        self.last_active_consumers: List[str] = []
        self.initial_read_done = False
        self.last_ids: Dict[str, str] = {}

    def connect(self) -> None:
        """Establish connection to Redis."""
        self.redis_client = redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            db=self.redis_db,
            password=self.redis_password,
            decode_responses=False,  # We'll handle decoding manually for better control
        )

        # Test the connection
        try:
            self.redis_client.ping()
            print(f'Connected to Redis at {self.redis_host}:{self.redis_port}')
        except redis.ConnectionError as e:
            raise ConnectionError(f'Failed to connect to Redis: {e}')

    def disconnect(self) -> None:
        """Close connection to Redis."""
        if self.redis_client:
            self.redis_client.close()
            self.redis_client = None

    def get_stream_name(self, partition: int) -> str:
        """Get the stream name for a specific partition."""
        return self.stream_name_template.format(partition)

    def create_consumer_groups(self) -> None:
        """Create consumer groups for all partition streams."""
        if not self.num_partitions:
            return

        if not self.redis_client:
            raise RuntimeError('Redis client not connected')

        if not self.group_name:
            return

        for partition in range(self.num_partitions):
            stream_name = self.get_stream_name(partition)
            try:
                self.redis_client.xgroup_create(
                    stream_name, self.group_name, id='0', mkstream=True
                )
                print(f'Created consumer group {self.group_name} for {stream_name}')
            except redis.RedisError as e:
                if 'BUSYGROUP' in str(e):
                    print(
                        f'Consumer group {self.group_name} already exists for {stream_name}'
                    )
                else:
                    print(f'Error creating group for {stream_name}: {e}')

    def setup_consumer(self) -> None:
        """Setup Redis consumer - register and start heartbeat thread."""

        # Start heartbeat thread
        if self.group_name:
            self.register_consumer()
            self.heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop, daemon=True
            )
            self.heartbeat_thread.start()
            self.last_active_consumers = self.get_active_consumers()

        # Initial partition assignment
        self.assigned_partitions = self._rebalance()
        print(
            f'Redis consumer setup complete, assigned partitions: {self.assigned_partitions}'
        )

    def cleanup_consumer(self) -> None:
        """Cleanup Redis consumer - unregister and stop heartbeat."""
        self.unregister_consumer()

    def register_consumer(self) -> None:
        """Register this consumer with Redis and trigger rebalance."""
        if not self.redis_client:
            raise RuntimeError('Redis client not connected')
        if not self.group_name:
            return

        self.redis_client.hset(self.consumers_key, self.consumer_name, time.time())
        self.redis_client.incr(self.rebalance_version_key)
        print(
            f'Registered consumer {self.consumer_name} in group {self.group_name}, triggered rebalance'
        )

    def unregister_consumer(self) -> None:
        """Unregister this consumer from Redis and trigger rebalance."""
        if not self.redis_client:
            raise RuntimeError('Redis client not connected')
        if not self.group_name:
            return

        self.redis_client.hdel(self.consumers_key, self.consumer_name)
        self.redis_client.incr(self.rebalance_version_key)
        print(
            f'Unregistered consumer {self.consumer_name} from group {self.group_name}, triggered rebalance'
        )

    def send_heartbeat(self) -> None:
        """Update the consumer's heartbeat timestamp in Redis."""
        if not self.redis_client:
            raise RuntimeError('Redis client not connected')

        self.redis_client.hset(self.consumers_key, self.consumer_name, time.time())

    def get_active_consumers(self) -> List[str]:
        """Get the list of active consumers, removing expired ones."""
        if not self.redis_client:
            raise RuntimeError('Redis client not connected')
        if not self.group_name:
            return []

        current_time = time.time()
        consumers = self.redis_client.hgetall(self.consumers_key)
        active_consumers = []
        removed = False

        for consumer_bytes, timestamp_bytes in consumers.items():
            consumer = consumer_bytes.decode()
            last_heartbeat = float(timestamp_bytes)
            if current_time - last_heartbeat < self.heartbeat_timeout:
                active_consumers.append(consumer)
            else:
                print(
                    f'Removing inactive consumer {consumer} from group {self.group_name}'
                )
                self.redis_client.hdel(self.consumers_key, consumer)
                removed = True

        if removed:
            self.redis_client.incr(
                self.rebalance_version_key
            )  # Trigger rebalance on timeout

        return active_consumers

    def assign_partitions(self, active_consumers: List[str]) -> List[int]:
        """
        Assign partitions to this consumer based on the current consumer group.

        Uses a simple round-robin assignment strategy.
        """
        if not self.num_partitions:
            return []

        if not self.redis_client:
            raise RuntimeError('Redis client not connected')

        if not self.group_name:
            return list(range(self.num_partitions))

        num_consumers = len(active_consumers)
        if num_consumers == 0:
            return []

        active_consumers = sorted(active_consumers)
        consumer_index = (
            active_consumers.index(self.consumer_name)
            if self.consumer_name in active_consumers
            else 0
        )

        assigned_partitions = []
        for partition in range(self.num_partitions):
            if partition % num_consumers == consumer_index:
                assigned_partitions.append(partition)

        self.redis_client.hset(
            self.partition_assignments_key,
            self.consumer_name,
            json.dumps(assigned_partitions),
        )
        print(
            f'Consumer {self.consumer_name} in group {self.group_name} assigned partitions: {assigned_partitions}'
        )
        return assigned_partitions

    def claim_pending_messages(self, assigned_partitions: List[int]) -> None:
        """Claim pending messages for the assigned partitions from other consumers."""
        if not self.redis_client:
            raise RuntimeError('Redis client not connected')

        if not self.group_name:
            return

        for partition in assigned_partitions:
            stream_name = self.get_stream_name(partition)
            try:
                pending = self.redis_client.xpending(stream_name, self.group_name)
                if not pending or pending['pending'] <= 0:
                    return
                    # Get pending message IDs for this partition stream
                messages = self.redis_client.xpending_range(
                    stream_name, self.group_name, '-', '+', count=10
                )
                message_ids_to_claim = []

                for msg in messages:
                    message_id = msg['message_id']
                    delivered_to = msg['consumer'].decode()
                    if delivered_to != self.consumer_name:
                        message_ids_to_claim.append(message_id)

                if not message_ids_to_claim:
                    return
                print(
                    f'Consumer {self.consumer_name} claiming {len(message_ids_to_claim)} messages from {stream_name}'
                )
                claimed_messages = self.redis_client.xclaim(
                    stream_name,
                    self.group_name,
                    self.consumer_name,
                    60000,  # min_idle_time in milliseconds
                    message_ids_to_claim,
                )

                if not claimed_messages:
                    return
                    # Process claimed messages
                for claimed_id, data in claimed_messages:
                    key = data[b'key'].decode()
                    message_data = json.loads(data[b'data'].decode())

                    # Extract trace context from raw message data and add to message_data
                    trace_context = self._extract_trace_context_from_raw_message(data)
                    if trace_context:
                        # Add trace context to message_data for processing
                        for trace_key, trace_value in trace_context.items():
                            message_data[f'_trace_{trace_key}'] = trace_value

                    self.process_message(claimed_id.decode(), key, message_data)
                    self.redis_client.xack(stream_name, self.group_name, claimed_id)

            except redis.exceptions.ResponseError as e:
                # Stream might not exist yet
                if 'no such key' not in str(e).lower():
                    print(f'Error claiming from {stream_name}: {e}')

    def read_messages(self) -> List[Tuple[str, str, Dict[str, Any]]]:
        """Read messages from assigned partitions using Redis Streams."""
        if not self.redis_client:
            raise RuntimeError('Redis client not connected')

        # Check for rebalance before reading
        if self.should_rebalance():
            print(
                f'Consumer {self.consumer_name} detected group change, rebalancing...'
            )
            self.assigned_partitions = self._rebalance()
            self.last_active_consumers = self.get_active_consumers()

        if not self.assigned_partitions:
            return []

        # Build stream dictionary for assigned partitions
        streams_to_read = {}
        # Determine which stream ID to use for reading
        if self.read_from_start and not self.initial_read_done:
            stream_id = '0'  # Read from beginning
            self.initial_read_done = True
        else:
            if self.group_name:
                stream_id = '>'  # Read only new messages
            else:
                stream_id = '$'

        for partition in self.assigned_partitions:
            stream_name = self.get_stream_name(partition)
            streams_to_read[stream_name] = stream_id

        if not self.group_name:
            if not self.last_ids:
                self.initial_read_done = True
                self.last_ids = {
                    stream_name: stream_id for stream_name in streams_to_read
                }
            else:
                # Use the last read message IDs instead of replacing entire dict
                for stream_name in streams_to_read:
                    if stream_name in self.last_ids:
                        streams_to_read[stream_name] = self.last_ids[stream_name]

        try:
            # Read from multiple partition streams at once
            if self.group_name:
                messages = self.redis_client.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams=streams_to_read,
                    count=1,
                    block=100,
                )
            else:
                messages = self.redis_client.xread(
                    streams=streams_to_read,
                    count=None,
                    block=100,  # Use small timeout instead of non-blocking
                )

            result = []
            if messages:
                # Process messages from each stream and track last message ID
                for stream_name_bytes, entries in messages:
                    stream_name = (
                        stream_name_bytes.decode()
                        if isinstance(stream_name_bytes, bytes)
                        else stream_name_bytes
                    )

                    last_message_id = None
                    for message_id_bytes, data in entries:
                        message_id = (
                            message_id_bytes.decode()
                            if isinstance(message_id_bytes, bytes)
                            else message_id_bytes
                        )
                        key = data[b'key'].decode()
                        message_data = json.loads(data[b'data'].decode())

                        # Extract trace context from raw message data and add to message_data
                        trace_context = self._extract_trace_context_from_raw_message(
                            data
                        )
                        if trace_context:
                            # Add trace context to message_data for processing
                            for trace_key, trace_value in trace_context.items():
                                message_data[f'_trace_{trace_key}'] = trace_value

                        result.append((message_id, key, message_data))
                        last_message_id = message_id

                    # Update last_ids with the latest message ID from this stream
                    # This ensures we don't miss messages between reads
                    if not self.group_name and last_message_id:
                        self.last_ids[stream_name] = last_message_id

            return result

        except redis.exceptions.ResponseError as e:
            print(f'Error reading streams: {e}')
            return []
        except Exception as e:
            print(f'Unexpected error reading messages: {e}')
            return []

    def acknowledge_message(self, message_id: str, **kwargs) -> None:
        """Acknowledge that a message has been processed."""
        if not self.redis_client:
            raise RuntimeError('Redis client not connected')

        # Extract partition from kwargs or derive from key
        key = kwargs.get('key', '')
        partition = self._extract_partition_from_message(message_id, key)
        stream_name = self.get_stream_name(partition)

        if not self.group_name:
            # For single consumers, stream position is tracked in read_messages()
            # No need to update last_ids here as it could cause race conditions
            return

        try:
            self.redis_client.xack(stream_name, self.group_name, message_id)
            print(f'Acknowledged message {message_id} from {stream_name}')
        except redis.exceptions.ResponseError as e:
            print(f'Error acknowledging message {message_id} from {stream_name}: {e}')

    def should_rebalance(self) -> bool:
        """Check if a rebalance is needed based on version changes or consumer group changes."""
        if not self.redis_client:
            raise RuntimeError('Redis client not connected')

        if not self.group_name:
            return False

        try:
            current_rebalance_version = int(
                self.redis_client.get(self.rebalance_version_key) or 0
            )
            current_active_consumers = self.get_active_consumers()

            version_changed = current_rebalance_version != self.last_rebalance_version
            consumers_changed = set(current_active_consumers) != set(
                self.last_active_consumers
            )

            if version_changed or consumers_changed:
                self.last_rebalance_version = current_rebalance_version
                return True

            return False
        except Exception as e:
            print(f'Error checking rebalance status: {e}')
            return False

    def process_message(
        self, message_id: str, key: str, message_data: Dict[str, Any]
    ) -> None:
        """
        Process a single message.

        If a message_processor callback was provided during initialization,
        it will be used. Otherwise, the default implementation writes to file.
        """
        # Start consumer span with tracing utilities
        with start_consumer_span(
            'redis-consume-message',
            message_data,
            messaging_system='redis',
            message_id=key,
            additional_attributes={
                'messaging.redis.message_id': message_id,
                'consumer.name': self.consumer_name,
                'consumer.group': self.group_name or '',
            },
        ):
            try:
                # Clean message data for processing (remove trace context)
                clean_data = clean_trace_context_from_message(message_data)

                if self.message_processor:
                    self.message_processor(message_id, key, clean_data)
                else:
                    # Default implementation - write to file
                    self.write_data(message_id, clean_data)

            except Exception:
                # Exception handling is done by the context manager
                raise

    def _extract_trace_context_from_raw_message(
        self, raw_message: Dict[bytes, bytes]
    ) -> Dict[str, str]:
        """Extract trace context from raw Redis message data."""
        trace_context = {}

        for key_bytes, value_bytes in raw_message.items():
            key = key_bytes.decode() if isinstance(key_bytes, bytes) else key_bytes
            if key.startswith('_trace_'):
                trace_key = key[7:]  # Remove '_trace_' prefix
                value = (
                    value_bytes.decode()
                    if isinstance(value_bytes, bytes)
                    else value_bytes
                )
                trace_context[trace_key] = value

        return trace_context

    def write_data(self, message_id: str, message: Dict[str, Any]) -> None:
        """Write message data to a file."""
        filename = f'data_{self.consumer_name}.txt'
        with open(filename, 'a') as f:
            f.write(f'{message_id} {message}\n')

    def _heartbeat_loop(self) -> None:
        """Periodically send heartbeats while the consumer is running."""
        while self.is_running:
            try:
                self.send_heartbeat()
                time.sleep(self.heartbeat_interval)
            except Exception as e:
                print(f'Error in heartbeat loop: {e}')
                time.sleep(self.heartbeat_interval)

    def _rebalance(self) -> List[int]:
        """Perform rebalancing and return assigned partitions."""
        if not self.redis_client:
            raise RuntimeError('Redis client not connected')

        if not self.num_partitions:
            return []
        if not self.group_name:
            return list(range(self.num_partitions))

        try:
            with self.redis_client.lock('rebalance_lock', timeout=10):
                active_consumers = self.get_active_consumers()
                if self.consumer_name not in active_consumers:
                    self.register_consumer()
                    active_consumers = self.get_active_consumers()

                assigned_partitions = self.assign_partitions(active_consumers)
                # self.claim_pending_messages(assigned_partitions)

                return assigned_partitions
        except Exception as e:
            print(f'Error during rebalance: {e}')
            return self.assigned_partitions

    def _extract_partition_from_message(self, message_id: str, key: str) -> int:
        """
        Extract partition number from message ID for Redis Streams.

        For Redis streams, we can extract the partition from the stream name
        or use the key hash as fallback.
        """
        # Try to extract from message_id if it contains stream info
        # Redis stream message IDs are in format: timestamp-sequence
        # We'll use the key hash approach for simplicity
        if not self.num_partitions:
            return 0

        if key:
            return hash(key) % self.num_partitions
        return 0


def create_redis_consumer(
    consumer_name: str,
    group_name: str,
    redis_host: str = 'localhost',
    redis_port: int = 6379,
    message_processor: Optional[Callable[[str, str, Dict[str, Any]], None]] = None,
    read_from_start: bool = False,
    **kwargs,
) -> RedisConsumer:
    """
    Factory function to create a RedisConsumer instance.

    Args:
        consumer_name: Unique name for this consumer instance
        group_name: Name of the consumer group
        redis_host: Redis server host
        redis_port: Redis server port
        message_processor: Optional callback function for processing messages
        read_from_start: If True, consumer will read messages from the beginning of the stream.
                       If False (default), consumer will only read new messages.
        **kwargs: Additional arguments passed to RedisConsumer constructor

    Returns:
        Configured RedisConsumer instance
    """
    return RedisConsumer(
        consumer_name=consumer_name,
        group_name=group_name,
        redis_host=redis_host,
        redis_port=redis_port,
        message_processor=message_processor,
        read_from_start=read_from_start,
        **kwargs,
    )
