# OpenHands Worker Consumer Package

This package provides a flexible and extensible consumer framework for the OpenHands worker system. It includes an abstract base class and concrete implementations for different messaging backends.

## Architecture

The consumer package follows an abstract base class pattern:

- **BaseConsumer**: Abstract base class defining the consumer interface
- **RedisConsumer**: Redis Streams-based implementation
- **KafkaConsumer**: Apache Kafka-based implementation (template)

## Features

- **Consumer Groups**: Support for consumer groups with automatic rebalancing
- **Partitioning**: Message partitioning for scalability
- **Heartbeat Management**: Automatic heartbeat and health monitoring
- **Graceful Shutdown**: Signal handling for clean shutdown
- **Pending Message Claims**: Automatic claiming of pending messages from failed consumers
- **Extensible**: Easy to extend for other messaging backends

## Quick Start

### Redis Consumer

```python
from openhands.worker.consumer import RedisConsumer

# Create and run a Redis consumer
consumer = RedisConsumer(
    consumer_name="worker_1",
    group_name="conversation_group",
    redis_host="localhost",
    redis_port=6379,
    num_partitions=4
)

consumer.run()  # Start consuming messages
```

### Kafka Consumer (Template)

```python
from openhands.worker.consumer import KafkaConsumer

# Create and run a Kafka consumer
consumer = KafkaConsumer(
    consumer_name="worker_1",
    group_name="conversation_group",
    bootstrap_servers="localhost:9092",
    topics=["conversation", "notifications"]  # Topics with partitions configured in Kafka
)

consumer.run()  # Start consuming messages
```

## Custom Consumer Implementation

To implement a custom consumer for a new messaging backend:

```python
from openhands.worker.consumer import BaseConsumer

class MyCustomConsumer(BaseConsumer):
    def connect(self):
        # Implement connection logic
        pass

    def register_consumer(self):
        # Implement consumer registration
        pass

    def read_messages(self, assigned_partitions):
        # Implement message reading
        pass

    # Implement other abstract methods...
```

## Consumer Configuration

### RedisConsumer Parameters

- `consumer_name`: Unique identifier for the consumer instance
- `group_name`: Consumer group name
- `redis_host`: Redis server hostname (default: 'localhost')
- `redis_port`: Redis server port (default: 6379)
- `redis_db`: Redis database number (default: 0)
- `redis_password`: Redis password (optional)
- `stream_name_template`: Template for stream names (default: "conversation_partition_{}")
- `num_partitions`: Number of partitions (default: 4)
- `heartbeat_interval`: Seconds between heartbeats (default: 5)
- `heartbeat_timeout`: Timeout for inactive consumers (default: 15)

### KafkaConsumer Parameters

- `consumer_name`: Unique identifier for the consumer instance
- `group_name`: Consumer group name
- `bootstrap_servers`: Kafka bootstrap servers (default: 'localhost:9092')
- `topics`: List of topics to subscribe to (default: ["conversation"])
- `kafka_config`: Additional Kafka configuration (optional)

Note: In Kafka, partitions are configured at the topic level, not by the consumer.

## Message Processing

Override the `process_message` method to customize message processing:

```python
class CustomConsumer(RedisConsumer):
    def process_message(self, message_id, key, message_data):
        # Custom processing logic
        print(f"Processing: {message_data}")

        # Call parent method for default behavior
        super().process_message(message_id, key, message_data)
```

## Running Examples

Run the example consumers:

```bash
# Redis consumer
python -m openhands.worker.consumer.example --type redis --name worker_1

# Kafka consumer (requires kafka-python)
python -m openhands.worker.consumer.example --type kafka --name worker_1
```

## Dependencies

### Redis Consumer
- `redis`: Python Redis client

### Kafka Consumer
- `kafka-python` OR `confluent-kafka`: Python Kafka client

Install dependencies:
```bash
# For Redis
pip install redis

# For Kafka (choose one)
pip install kafka-python
# OR
pip install confluent-kafka
```

## Message Format

Messages should follow this format:

```json
{
    "key": "message_key",
    "data": {
        "conversation_id": "conv_123",
        "message": "Hello world",
        "timestamp": 1234567890,
        "priority": "normal"
    }
}
```

## Consumer Groups and Rebalancing

The consumer package supports automatic rebalancing when:
- New consumers join the group
- Existing consumers leave or become inactive
- Consumer heartbeats timeout

Partitions are automatically redistributed among active consumers using a round-robin strategy.

## Error Handling

The consumer framework includes robust error handling:
- Connection failures are automatically retried
- Message processing errors are logged but don't stop the consumer
- Graceful shutdown on SIGINT/SIGTERM signals
- Automatic cleanup of inactive consumers

## Monitoring

Consumers provide built-in monitoring through:
- Heartbeat messages
- Partition assignment logging
- Message processing statistics
- Error reporting

## Testing

The package includes comprehensive tests and examples based on the original `test_consumer_v2.py` implementation.

## Contributing

When adding new consumer implementations:

1. Inherit from `BaseConsumer`
2. Implement all abstract methods
3. Add appropriate error handling
4. Include comprehensive docstrings
5. Add factory functions for easy instantiation
6. Update the `__init__.py` file to export new classes
