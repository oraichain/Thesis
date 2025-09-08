# OpenHands Server - Distributed Conversation Processing

This document describes the distributed architecture for conversation processing in OpenHands when running in multi-worker mode.

## Architecture Overview

The distributed architecture separates the API layer (handling client connections) from the worker layer (processing conversations) using a message queue system. This enables horizontal scaling and better resource utilization.

### Components

#### 1. API Server
- **Maintains conversation manager** (but doesn't process controller)
- **Maintains API event stream** for sending data to clients
- **Publishes NewConversationEvent** to message queue when new conversations are created
- **Runs API Consumer** to listen for ProcessConversationEvent and forward to API event stream

#### 2. Worker
- **Runs Conversation Consumer** that listens for NewConversationEvent
- **Processes conversations** using conversation manager and controller
- **Maintains Worker Event Stream** for conversation processing
- **Publishes ProcessConversationEvent** to message queue as conversation progresses
- **Event Forwarder** listens to Worker Event Stream and publishes updates

#### 3. Message Queue
- **Redis Streams** for reliable message queuing
- **Partitioning** for scalability across multiple workers
- **Consumer Groups** for load balancing

## Message Types

### NewConversationEvent
**Published by:** API Server
**Consumed by:** Worker Conversation Consumer
```python
{
    "conversation_id": "conv_123",
    "event_type": "new_conversation",
    "conversation_init_data": {...},
    "user_id": "user_123",
    "initial_user_msg": "Hello",
    # ... other conversation parameters
}
```

### ProcessConversationEvent
**Published by:** Worker
**Consumed by:** API Server
```python
{
    "conversation_id": "conv_123",
    "event_type": "process_conversation",
    "event_data": {...},
    "source": "worker_001",
    "status": "processing",
    # ... progress information
}
```

### ConversationCompleteEvent
**Published by:** Worker
**Consumed by:** API Server
```python
{
    "conversation_id": "conv_123",
    "event_type": "conversation_complete",
    "success": true,
    "final_result": "...",
    "processing_time": 15.5,
    "total_tokens": 1250
}
```

### ConversationErrorEvent
**Published by:** Worker
**Consumed by:** API Server
```python
{
    "conversation_id": "conv_123",
    "event_type": "conversation_error",
    "error_type": "processing_error",
    "error_message": "Failed to process conversation",
    "recoverable": false
}
```

## Event Flow

### 1. Conversation Creation
```
Client Request → API Server → Publish NewConversationEvent → Worker Consumer
```

### 2. Conversation Processing
```
Worker Consumer → Start Conversation → Worker Event Stream → Event Forwarder → Publish ProcessConversationEvent → API Consumer → API Event Stream → Client
```

### 3. Conversation Completion
```
Worker → Publish ConversationCompleteEvent → API Consumer → API Event Stream → Client
```

### 4. Error Handling
```
Worker → Publish ConversationErrorEvent → API Consumer → API Event Stream → Client
```

## Usage

### Running API Server with Multi-Worker Support

```bash
# Set worker mode to multi-worker
export WORKER_MODE=multi_worker
export WORKER_QUEUE_HOST=localhost
export WORKER_QUEUE_PORT=6379

# Run API server
python -m openhands.server.app
```

### Running Workers

```bash
# Run conversation consumer worker
python -m openhands.server.conversation_consumer
```

### Running API Consumer Service

The API consumer service runs automatically as a background thread within the API server process when in multi-worker mode:

```bash
# Set multi-worker mode
export WORKER_MODE=multi_worker

# Start API server - API consumer service starts automatically as background thread
python -m openhands.server.app
```

**Key Benefits:**
- **Single Process**: No need to manage separate consumer processes
- **Resource Efficiency**: Shared memory and connection pools
- **Tight Integration**: Direct access to server's conversation management
- **Automatic Lifecycle**: Starts and stops with the API server
- **Thread Safety**: Proper synchronization with server's event loop

## Architecture Details

### Thread-Based API Consumer Service

In the recommended setup, the API consumer runs as a background thread within the same process as the API server. This provides several advantages:

#### Benefits
- **Simplified Deployment**: No need to manage separate processes or containers
- **Resource Efficiency**: Shared memory, connection pools, and context
- **Tight Integration**: Direct access to server's conversation management
- **Better Observability**: All logs and metrics in one place
- **Fault Isolation**: If the API consumer thread fails, it doesn't bring down the server

#### How It Works
1. **Server Startup**: When the API server starts in multi-worker mode, it automatically creates and starts the API consumer service
2. **Background Thread**: The consumer runs in a separate thread, listening for messages without blocking the main server
3. **Event Forwarding**: When worker events are received, they're forwarded to the appropriate conversation's event stream
4. **Graceful Shutdown**: When the server shuts down, the consumer thread is cleanly stopped

#### Thread Safety
- The consumer thread uses message polling with timeouts to avoid blocking
- Event forwarding is scheduled as async tasks in the server's event loop
- Proper synchronization ensures thread-safe access to shared resources

## Configuration

### Worker Configuration
```toml
[worker]
mode = "multi_worker"
queue_type = "redis"
queue_host = "localhost"
queue_port = 6379
queue_db = 0
queue_num_partitions = 4
```

### Environment Variables
- `WORKER_MODE`: `standalone` or `multi_worker`
- `WORKER_QUEUE_HOST`: Redis host
- `WORKER_QUEUE_PORT`: Redis port
- `WORKER_QUEUE_DB`: Redis database number
- `WORKER_QUEUE_PASSWORD`: Redis password (optional)

## Scaling

### Horizontal Scaling
- **Multiple Workers**: Run multiple conversation consumers for load balancing
- **Multiple API Servers**: Run multiple API servers with different consumer groups
- **Partitioning**: Redis streams are partitioned for better performance

### Consumer Groups
- **conversation_workers**: Worker consumers processing NewConversationEvent
- **api_consumers**: API consumers processing ProcessConversationEvent

## Error Handling

### Retry Logic
- Failed message processing can be retried
- Dead letter queues for persistent failures
- Exponential backoff for recoverable errors

### Monitoring
- Conversation processing metrics
- Queue depth monitoring
- Error rate tracking
- Worker health monitoring

## Benefits

1. **Scalability**: Separate API and worker layers allow independent scaling
2. **Resource Efficiency**: Workers can be optimized for processing, API servers for client handling
3. **Reliability**: Message queue provides decoupling and fault tolerance
4. **Observability**: Clear separation of concerns for monitoring and debugging
5. **Flexibility**: Easy to add new worker types or API consumers

## Development

### Adding New Event Types
1. Define event class in `openhands/core/events/conversation_events.py`
2. Add factory function for creating the event
3. Update consumer `process_message` methods to handle the new event type
4. Update documentation

### Testing
- Unit tests for individual components
- Integration tests for event flow
- Load testing for scalability validation

## Troubleshooting

### Common Issues

1. **Messages not being consumed**: Check Redis connection and consumer group status
2. **Events not reaching clients**: Verify API consumer is running and connected
3. **Worker not processing**: Check NewConversationEvent publishing and worker logs
4. **Performance issues**: Monitor queue depth and add more workers/partitions

### Debugging
- Enable debug logging for detailed event flow
- Use Redis CLI to inspect queue contents
- Monitor consumer lag and pending messages
