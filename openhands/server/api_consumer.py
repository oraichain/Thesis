"""
API Consumer Service - Background Thread for Processing Conversation Events

This service runs the message consumer as a background thread within the API server process,
providing seamless integration with the server's conversation management and event streaming.
The consumer type (Redis, Kafka, etc.) is determined by the worker configuration.
"""

import asyncio
import threading
from typing import Any, Callable, Dict, Optional

from opentelemetry import trace

from openhands.core.events.conversation_events import (
    ConversationEventType,
    ProcessConversationEvent,
)
from openhands.core.logger import openhands_logger as logger
from openhands.events.action.message import MessageAction
from openhands.events.event import Event, EventSource
from openhands.events.serialization.event import event_from_dict
from openhands.server.conversation_manager.conversation_manager import (
    ConversationManager,
)
from openhands.utils.async_utils import _run_in_loop
from openhands.utils.messaging_tracing import (
    clean_trace_context_from_message,
    start_consumer_span,
)
from openhands.worker.consumer import BaseConsumer

trace_provider = trace.get_tracer_provider()
tracer = trace_provider.get_tracer(__name__)


class ServerAPIConsumeProcessor:
    """
    Message processor for handling conversation events from workers on the API server.

    This class provides the logic for processing conversation events and forwarding
    them to the appropriate API event streams to send data to clients. It is designed
    to be used as a callback function injected into a RedisConsumer instance.
    """

    def __init__(
        self, conversation_manager: ConversationManager, loop: asyncio.AbstractEventLoop
    ):
        """
        Initialize the API consumer processor.

        Args:
            conversation_manager: Server's conversation manager instance (optional)
        """
        if not conversation_manager:
            raise ValueError('Conversation manager is required')

        self.conversation_manager: ConversationManager = conversation_manager
        self._event_streams: Dict[str, Any] = {}  # conversation_id -> event_stream
        self.loop = loop

    def get_message_processor(self) -> Callable[[str, str, Dict[str, Any]], None]:
        """
        Get the message processor callback function that can be injected into RedisConsumer.

        Returns:
            Callable that processes conversation event messages
        """
        return self.process_conversation_message

    def process_conversation_message(
        self, message_id: str, key: str, message_data: Dict[str, Any]
    ) -> None:
        """
        Process a conversation event message from workers.

        This method serves as the callback function for RedisConsumer's message processing.
        It handles ProcessConversationEvent.

        Args:
            message_id: Unique identifier for the message
            key: Message key
            message_data: Message payload containing event data
        """
        type_process_mapping = {
            ConversationEventType.PROCESS_CONVERSATION: self._handle_process_conversation_event,
        }

        try:
            logger.info(
                f'Processing conversation event message: {message_id}',
                extra={'message_id': message_id, 'key': key},
            )

            # Determine event type and parse accordingly
            event_type = message_data.get('event_type')
            conversation_id = message_data.get('conversation_id')

            if not conversation_id:
                logger.error('No conversation_id found in message')
                return

            if event_type in type_process_mapping:
                # Start consumer span with tracing utilities
                with start_consumer_span(
                    'api-consumer-consume-message',
                    message_data,
                    messaging_system='redis',
                    message_id=key,
                    additional_attributes={
                        'api_consumer.event_type': event_type,
                        'api_consumer.conversation_id': conversation_id or '',
                    },
                ):
                    # Clean message data for processing (remove trace context)
                    clean_data = clean_trace_context_from_message(message_data)
                    type_process_mapping[event_type](clean_data)
            else:
                logger.warning(f'Unknown event type: {event_type}')

            logger.info(
                f'Successfully processed conversation event: {event_type} for {conversation_id}',
                extra={'conversation_id': conversation_id, 'message_id': message_id},
            )

        except Exception as e:
            logger.error(
                f'Error processing conversation event message {message_id}: {e}',
                extra={'message_id': message_id, 'key': key},
            )
            # In a production system, you might want to implement retry logic or dead letter queue
            raise

    def _handle_process_conversation_event(self, message_data: Dict[str, Any]) -> None:
        """
        Handle ProcessConversationEvent by forwarding to API event stream.

        Args:
            event: The ProcessConversationEvent to handle
        """
        try:
            event = ProcessConversationEvent(**message_data)
            print('message_data', message_data)

            # Forward the event data to the appropriate API event stream
            if not self.loop.is_running():
                self.loop.run_until_complete(self._forward_to_api_event_stream(event))
            else:
                _run_in_loop(
                    self._forward_to_api_event_stream(event),
                    loop=self.loop,
                    timeout=100,
                )

        except Exception as e:
            logger.error(
                f'Error handling ProcessConversationEvent for {event.conversation_id}: {e}',
                extra={'conversation_id': event.conversation_id},
            )

    async def _forward_to_api_event_stream(
        self, event: ProcessConversationEvent
    ) -> None:
        """
        Forward ProcessConversationEvent to the appropriate API event stream.

        Args:
            event: The event to forward
        """
        try:
            # Get or create event stream for this conversation
            event_stream = self.conversation_manager.get_event_stream(
                event.conversation_id
            )

            if not event_stream:
                logger.info(
                    f'No event stream available for conversation {event.conversation_id}',
                    extra={'conversation_id': event.conversation_id},
                )
                return

            event_obj, event_source = self._convert_event_data(event.event_data)
            event_stream.stream_event_only(event_obj, event_source)
            print(
                f'Session: event_stream {id(event_stream)} subscribers: {event_stream._subscribers}'
            )
            logger.info(f'Event subscribers: {event_stream._subscribers}')
            logger.info(
                f'Forwarded event data {event_obj} to API event stream for {event.conversation_id}',
                extra={'conversation_id': event.conversation_id},
            )

        except Exception as e:
            logger.error(
                f'Error forwarding to API event stream for {event.conversation_id}: {e}',
                extra={'conversation_id': event.conversation_id},
            )

    def _convert_event_data(
        self, event_data: Dict[str, Any]
    ) -> tuple[Event, EventSource]:
        """
        Async function to convert event data from dict to Event and EventSource.

        Args:
            conversation_id: The ID of the conversation
            event_data: The event data to forward
        """
        event_obj = None
        event_source = EventSource.AGENT  # Default source

        try:
            event_obj = event_from_dict(event_data)

            # Extract the source from the event data if available
            if 'source' in event_data:
                try:
                    event_source = EventSource(event_data['source'])
                except ValueError:
                    # If invalid source, keep the default
                    pass

            # Set the source on the event object if it has the attribute
            if hasattr(event_obj, '_source'):
                event_obj._source = event_source

        except Exception as e:
            # Fallback to the old behavior if deserialization fails
            logger.warning(
                f'Failed to deserialize event data, falling back to MessageAction: {e}',
            )

            event_obj = MessageAction(
                content=event_data.get('message', ''),
                image_urls=event_data.get('image_urls', []),
                mode=event_data.get('mode'),
            )
        return event_obj, event_source


class APIConsumerService:
    """
    Background service that runs message consumer as a thread within the server process.

    This service:
    - Runs message consumer (Redis, Kafka, etc.) in a background thread based on configuration
    - Integrates with server's conversation management
    - Provides event forwarding to API event streams
    - Handles graceful shutdown
    """

    def __init__(
        self,
        consumer: BaseConsumer,
        conversation_manager: ConversationManager,
        consumer_name: str = 'api_consumer_server',
    ):
        """
        Initialize the API consumer service.

        Args:
            consumer: Injected consumer instance (Redis, Kafka, etc.)
            conversation_manager: Server's conversation manager instance
            consumer_name: Name for the consumer instance
        """
        if not conversation_manager:
            raise ValueError('Conversation manager is required')

        self.consumer = consumer
        self.conversation_manager = conversation_manager
        self.consumer_name = consumer_name

        # Store for tracking active conversations
        self.active_conversations: Dict[str, Any] = {}

        self.consumer_thread: Optional[threading.Thread] = None

        self.running = False
        self._shutdown_event = threading.Event()

    def start(self) -> None:
        """Start the API consumer service in a background thread."""
        if self.running:
            logger.warning('API consumer service is already running')
            return

        logger.info(f'Starting API consumer service: {self.consumer_name}')

        self.running = True
        self._shutdown_event.clear()

        # Use the injected Redis consumer
        # Start the consumer in a background thread
        self.consumer_thread = threading.Thread(
            target=self._run_consumer,
            name=f'APIConsumer-{self.consumer_name}',
            daemon=True,
        )
        self.consumer_thread.start()

        logger.info(f'API consumer service started successfully: {self.consumer_name}')

    def _run_consumer(self) -> None:
        """Run the consumer in the background thread."""
        try:
            logger.info(f'API consumer thread started: {self.consumer_name}')

            # Use the standard consumer run loop
            # The message processing will be handled by the injected callback
            if self.consumer:
                self.consumer.run()

        except Exception as e:
            logger.error(f'Fatal error in consumer thread: {e}')
        finally:
            logger.info(f'API consumer thread stopped: {self.consumer_name}')

    def stop(self) -> None:
        """Stop the API consumer service."""
        if not self.running:
            logger.warning('API consumer service is not running')
            return

        logger.info(f'Stopping API consumer service: {self.consumer_name}')

        self.running = False
        self._shutdown_event.set()

        # Wait for the consumer thread to finish
        if self.consumer_thread and self.consumer_thread.is_alive():
            self.consumer_thread.join(timeout=5.0)
            if self.consumer_thread.is_alive():
                logger.warning('Consumer thread did not stop gracefully')

        # Cleanup consumer
        if self.consumer:
            try:
                self.consumer.stop()
            except Exception as e:
                logger.error(f'Error stopping consumer: {e}')

        logger.info(f'API consumer service stopped: {self.consumer_name}')

    def is_running(self) -> bool:
        """Check if the service is running."""
        return bool(
            self.running and self.consumer_thread and self.consumer_thread.is_alive()
        )

    def get_status(self) -> Dict[str, Any]:
        """Get the current status of the service."""
        return {
            'running': self.is_running(),
            'consumer_name': self.consumer_name,
            'thread_alive': self.consumer_thread.is_alive()
            if self.consumer_thread
            else False,
            'consumer_connected': self.consumer is not None
            and hasattr(self.consumer, 'redis_client')
            and self.consumer.redis_client is not None,
        }


def create_api_consumer_service(
    conversation_manager: ConversationManager,
    consumer_name: str = 'api_consumer_server',
) -> APIConsumerService:
    """
    Factory function to create an APIConsumerService instance with injected Redis consumer.

    This service runs the API consumer as a background thread within the API server process,
    providing seamless integration with the server's conversation management and event streaming.

    Args:
        conversation_manager: Server's conversation manager instance
        consumer_name: Name for the consumer instance

    Returns:
        APIConsumerService: Configured service instance with injected consumer
    """
    # Import here to avoid circular imports
    from openhands.shared import config
    from openhands.worker.consumer import create_consumer_from_config

    # Create the message processor instance with conversation manager
    try:
        loop = asyncio.get_event_loop()
    except Exception:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    message_processor_instance = ServerAPIConsumeProcessor(
        conversation_manager=conversation_manager, loop=loop
    )
    message_processor_callback = message_processor_instance.get_message_processor()

    # Create the consumer based on configuration with the message processor callback
    consumer = create_consumer_from_config(
        consumer_name=consumer_name,
        group_name='',
        config=config,
        message_processor=message_processor_callback,
        read_from_start=False,
    )

    # Create and return the service with injected consumer
    return APIConsumerService(
        consumer=consumer,
        conversation_manager=conversation_manager,
        consumer_name=consumer_name,
    )
