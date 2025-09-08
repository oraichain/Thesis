"""
Conversation Consumer for processing conversation creation events.

This consumer listens for conversation creation events published to the message queue
and processes them by starting conversations using the conversation manager.

The ConversationConsumer uses dependency injection to work with different message
queue technologies (Redis, Kafka, etc.) without being tightly coupled to any specific
implementation.

Example usage:

    # Using Redis (traditional approach)
    consumer = create_conversation_consumer_with_redis(
        consumer_name="worker_1",
        redis_host="localhost",
        redis_port=6379
    )
    consumer.run()

    # Using dependency injection with custom implementations
    my_consumer = MyCustomMessageConsumer()
    my_publisher = MyCustomMessagePublisher()
    consumer = ConversationConsumer(
        consumer_name="worker_1",
        message_consumer=my_consumer,
        publisher=my_publisher
    )
    consumer.run()
"""

import asyncio
import os
import signal
import sys
import time
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Any, Dict

from dotenv import load_dotenv
from opentelemetry import trace

trace_provider = trace.get_tracer_provider()
tracer = trace_provider.get_tracer(__name__)


load_dotenv()
try:
    sys.path.pop(0)
except Exception:
    pass
sys.path.append(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir)
)

if os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT'):
    # httpx instrumentation need start before any httpx client is created
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    from opentelemetry.instrumentation.redis import RedisInstrumentor

    HTTPXClientInstrumentor().instrument()
    RedisInstrumentor().instrument()


import openhands.agenthub  # noqa F401 (we import this to get the agents registered)
from openhands.core.events.conversation_events import (  # noqa
    NewConversationEvent,
    ConversationEventType,
    UserActionEvent,
    create_process_conversation_event,
)  # noqa
from openhands.core.logger import openhands_logger as logger  # noqa
from openhands.events.action.message import MessageAction  # noqa
from openhands.server.session.conversation_init_data import ConversationInitData  # noqa
from openhands.worker.consumer import BaseConsumer, create_consumer_from_config  # noqa
from openhands.worker.publisher import BasePublisher, create_publisher_from_config  # noqa
from openhands.server.conversation_manager.conversation_manager import (  # noqa
    ConversationManager,
)  # noqa
from openhands.events.stream import EventStreamSubscriber  # noqa
from openhands.events.serialization.event import event_to_dict  # noqa
from openhands.events.event import Event  # noqa
from openhands.server.db import connect_database  # noqa
from openhands.server.mcp_cache import mcp_tools_cache  # noqa
from openhands.server.shared import config  # noqa
from openhands.server.backend_pre_start import init  # noqa
from openhands.server.db import engine  # noqa
from openhands.server.initial_data import init as init_initial_data  # noqa
from openhands.utils.get_user_setting import settings_for_conversation, get_user_setting  # noqa
from openhands.utils.async_utils import _run_in_loop, run_loop_in_thread  # noqa
from openhands.utils.messaging_tracing import (  # noqa
    start_consumer_span,
    start_producer_span,
    clean_trace_context_from_message,
)  # noqa
from openhands.core.schema import AgentState  # noqa


class ConversationConsumer:
    """
    Consumer for processing conversation creation events from the message queue.

    This consumer listens for conversation creation messages and processes them by
    starting conversations using the conversation manager. It uses dependency injection
    to work with different message queue technologies.
    """

    def __init__(
        self,
        consumer_name: str,
        message_consumer: BaseConsumer,
        publisher: BasePublisher,
        conversation_manager: ConversationManager,
        shutdown_timeout: int = 300,  # 5 minutes default timeout
    ):
        """
        Initialize the conversation consumer.

        Args:
            consumer_name: Unique name for this consumer instance
            message_consumer: The message consumer implementation to use
            publisher: Optional message publisher for sending events
            conversation_manager: Conversation manager instance
            shutdown_timeout: Maximum time to wait for conversations to finish during shutdown (seconds)
        """
        if not message_consumer:
            raise ValueError('Message consumer is required')
        if not publisher:
            raise ValueError('Publisher is required')
        if not conversation_manager:
            raise ValueError('Conversation manager is required')

        self.consumer_name = consumer_name
        self.message_consumer = message_consumer
        self.publisher = publisher
        self.conversation_manager = conversation_manager
        self.shutdown_timeout = shutdown_timeout
        self._shutdown_initiated = False
        self._stopped = False

        try:
            self.loop = asyncio.get_event_loop()
        except Exception:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        if not self.loop.is_running():
            run_loop_in_thread(self.loop)

        # Set up graceful shutdown signal handlers
        self._setup_graceful_shutdown_handlers()

    def start(self) -> None:
        """Start the conversation consumer."""
        self.publisher.start()
        _run_in_loop(self.setup(), loop=self.loop, timeout=10)

    async def setup(self) -> None:
        await init(engine)
        await init_initial_data()

        if not mcp_tools_cache.is_loaded:
            await mcp_tools_cache.initialize_tools(
                config.dict_mcp_config, config.dict_search_engine_config
            )

    def _setup_graceful_shutdown_handlers(self) -> None:
        """Set up signal handlers for graceful shutdown."""

        def graceful_shutdown_handler(signum, frame):
            signal_name = 'SIGTERM' if signum == signal.SIGTERM else 'SIGINT'
            logger.info(
                f'Received {signal_name}, initiating graceful shutdown of consumer: {self.consumer_name}'
            )
            self.graceful_stop()

        # Override the base consumer's signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, graceful_shutdown_handler)
        signal.signal(signal.SIGINT, graceful_shutdown_handler)

    def graceful_stop(self) -> None:
        """Initiate graceful shutdown - stop consuming new messages and wait for conversations to finish."""
        if self._shutdown_initiated:
            logger.warning(
                f'Graceful shutdown already initiated for consumer: {self.consumer_name}'
            )
            return

        self._shutdown_initiated = True
        logger.info(
            f'Starting graceful shutdown for conversation consumer: {self.consumer_name}'
        )

        try:
            # Schedule graceful shutdown in the event loop thread
            future = asyncio.run_coroutine_threadsafe(
                self._graceful_shutdown_async(), self.loop
            )
            # Wait for completion with timeout
            future.result(timeout=self.shutdown_timeout + 30)
        except (FutureTimeoutError, TimeoutError) as e:
            logger.error(f'Graceful shutdown timed out: {e}')
            # Fall back to immediate stop
            self.stop()
        except Exception as e:
            logger.error(f'Error during graceful shutdown: {e}')
            # Fall back to immediate stop
            self.stop()

    async def _graceful_shutdown_async(self) -> None:
        """Async implementation of graceful shutdown."""
        # Step 1: Stop the message consumer to prevent new messages
        logger.info(
            f'Step 1: Stopping message consumer to prevent new messages: {self.consumer_name}'
        )
        try:
            self.message_consumer.stop()
            logger.info(f'Message consumer stopped successfully: {self.consumer_name}')
        except Exception as e:
            logger.error(f'Error stopping message consumer: {e}')

        # Step 2: Wait for all active conversations to finish
        logger.info(
            f'Step 2: Waiting for active conversations to finish: {self.consumer_name}'
        )
        await self._wait_for_conversations_to_finish()

        # Step 3: Stop the publisher and cleanup
        logger.info(f'Step 3: Stopping publisher and cleanup: {self.consumer_name}')
        if self.publisher:
            try:
                self.publisher.stop()
                logger.info(f'Publisher stopped successfully: {self.consumer_name}')
            except Exception as e:
                logger.error(f'Error stopping publisher: {e}')

        # Step 4: Shutdown conversation manager
        logger.info(f'Step 4: Shutting down conversation manager: {self.consumer_name}')
        await self._shutdown_conversation_manager()

        # Step 5: Stop the event loop
        logger.info(f'Step 5: Stopping event loop: {self.consumer_name}')
        await self._stop_event_loop()

        logger.info(f'Graceful shutdown completed for consumer: {self.consumer_name}')
        self._stopped = True

    async def _wait_for_conversations_to_finish(self) -> None:
        """Wait for all active conversations to finish with timeout."""
        start_time = time.time()
        check_interval = 5  # Check every 5 seconds

        running_state = [
            AgentState.RUNNING,
            AgentState.LOADING,
        ]
        while time.time() - start_time < self.shutdown_timeout:
            try:
                # Get all running agent loops
                running_loops = await self.conversation_manager.get_running_agent_loops(
                    filter_to_states=running_state
                )

                if not running_loops:
                    logger.info(
                        f'All conversations finished, proceeding with shutdown: {self.consumer_name}'
                    )
                    return

                elapsed_time = time.time() - start_time
                remaining_time = self.shutdown_timeout - elapsed_time

                logger.info(
                    f'Waiting for {len(running_loops)} active conversations to finish. '
                    f'Time remaining: {remaining_time:.1f}s. Active sessions: {list(running_loops)}'
                )

                # Wait before next check
                await asyncio.sleep(min(check_interval, remaining_time))

            except Exception as e:
                logger.error(f'Error checking active conversations: {e}')
                await asyncio.sleep(check_interval)

        # Timeout reached
        try:
            running_loops = await self.conversation_manager.get_running_agent_loops(
                filter_to_states=running_state
            )
            if running_loops:
                logger.warning(
                    f'Shutdown timeout ({self.shutdown_timeout}s) reached. '
                    f'Force closing {len(running_loops)} remaining conversations: {list(running_loops)}'
                )
                # Force close remaining conversations
                await self._force_close_conversations(running_loops)
            else:
                logger.info(
                    f'All conversations finished during final check: {self.consumer_name}'
                )
        except Exception as e:
            logger.error(f'Error during timeout handling: {e}')

    async def _force_close_conversations(self, conversation_ids: set[str]) -> None:
        """Force close conversations that didn't finish within timeout."""
        for conversation_id in conversation_ids:
            try:
                logger.info(f'Force closing conversation: {conversation_id}')
                await self.conversation_manager.close_session(conversation_id)
            except Exception as e:
                logger.error(f'Error force closing conversation {conversation_id}: {e}')

    async def _shutdown_conversation_manager(self) -> None:
        """Properly shutdown the conversation manager."""
        try:
            # Check if the conversation manager is an async context manager
            if hasattr(self.conversation_manager, '__aexit__'):
                logger.info(
                    f'Shutting down conversation manager async context: {self.consumer_name}'
                )
                await self.conversation_manager.__aexit__(None, None, None)
                logger.info(
                    f'Conversation manager shutdown completed: {self.consumer_name}'
                )
            else:
                logger.info(
                    f'Conversation manager does not support async context shutdown: {self.consumer_name}'
                )

            # Additional cleanup for StandaloneConversationManager
            if hasattr(self.conversation_manager, '_cleanup_task'):
                cleanup_task = getattr(self.conversation_manager, '_cleanup_task')
                if cleanup_task and not cleanup_task.done():
                    logger.info(
                        f'Cancelling conversation manager cleanup task: {self.consumer_name}'
                    )
                    cleanup_task.cancel()
                    try:
                        await cleanup_task
                    except asyncio.CancelledError:
                        logger.info(
                            f'Cleanup task cancelled successfully: {self.consumer_name}'
                        )

            # Force close any remaining sessions
            if hasattr(self.conversation_manager, '_local_agent_loops_by_sid'):
                remaining_sessions = getattr(
                    self.conversation_manager, '_local_agent_loops_by_sid'
                )
                if remaining_sessions:
                    logger.info(
                        f'Force closing {len(remaining_sessions)} remaining sessions: {self.consumer_name}'
                    )
                    for sid in list(remaining_sessions.keys()):
                        try:
                            await self.conversation_manager.close_session(sid)
                        except Exception as e:
                            logger.error(f'Error closing session {sid}: {e}')

        except Exception as e:
            logger.error(f'Error shutting down conversation manager: {e}')

    async def _stop_event_loop(self) -> None:
        """Stop the event loop properly."""
        try:
            # Schedule the loop to stop after this coroutine completes
            def stop_loop():
                if self.loop and self.loop.is_running():
                    logger.info(
                        f'Stopping event loop for consumer: {self.consumer_name}'
                    )
                    self.loop.stop()

            # Schedule the stop to happen after a brief delay to allow this coroutine to complete
            self.loop.call_later(0.1, stop_loop)

        except Exception as e:
            logger.error(f'Error stopping event loop: {e}')

    def stop(self) -> None:
        """Stop the conversation consumer immediately (non-graceful)."""
        if self._stopped:
            logger.debug(f'Consumer {self.consumer_name} already stopped, skipping')
            return

        logger.info(f'Stopping conversation consumer immediately: {self.consumer_name}')
        self._stopped = True

        # Stop publisher
        if self.publisher:
            try:
                self.publisher.stop()
            except Exception as e:
                logger.error(f'Error stopping publisher: {e}')

        # Stop message consumer
        try:
            self.message_consumer.stop()
        except Exception as e:
            logger.error(f'Error stopping message consumer: {e}')

        # Stop conversation manager
        try:
            _run_in_loop(
                self._shutdown_conversation_manager(),
                loop=self.loop,
                timeout=30,  # Short timeout for immediate stop
                is_waiting=True,
            )
        except Exception as e:
            logger.error(f'Error stopping conversation manager: {e}')

        # Stop event loop
        try:
            if self.loop and self.loop.is_running():
                logger.info(
                    f'Stopping event loop for immediate shutdown: {self.consumer_name}'
                )
                self.loop.call_soon_threadsafe(self.loop.stop)
        except Exception as e:
            logger.error(f'Error stopping event loop: {e}')

    @connect_database
    def run(self) -> None:
        """Run the conversation consumer processing loop."""
        try:
            self.start()

            # Set up message processing - if the consumer supports it, inject our process_message method
            if hasattr(self.message_consumer, 'message_processor'):
                self.message_consumer.message_processor = self.process_message

            self.message_consumer.run()
        except Exception as e:
            # Check if this is expected due to graceful shutdown
            if self._stopped:
                logger.info(
                    f'Consumer {self.consumer_name} stopped during graceful shutdown'
                )
            else:
                # Check for expected errors during shutdown
                error_msg = str(e).lower()
                if (
                    'i/o operation on closed file' in error_msg
                    or 'redis client not connected' in error_msg
                ):
                    logger.info(
                        f'Consumer {self.consumer_name} encountered expected shutdown error: {e}'
                    )
                else:
                    logger.error(
                        f'Unexpected error in consumer {self.consumer_name}: {e}'
                    )
                    raise
        finally:
            # Only call stop if we haven't already stopped gracefully
            if not self._stopped:
                self.stop()

    def process_message(
        self, message_id: str, key: str, message_data: Dict[str, Any]
    ) -> None:
        """
        Process a conversation event message.

        Args:
            message_id: Unique identifier for the message
            key: Message key
            message_data: Message payload containing event data
        """
        type_process_mapping = {
            ConversationEventType.NEW_CONVERSATION: self._handle_new_conversation_event,
            ConversationEventType.USER_ACTION: self._handle_user_action_event,
        }
        try:
            logger.info(
                f'Processing conversation message: {message_id}',
                extra={'message_id': message_id, 'key': key},
            )

            # Check event type
            event_type = message_data.get('event_type')
            if event_type not in type_process_mapping:
                logger.warning(f'Unknown event type: {event_type}')
                return
            # Start consumer span with tracing utilities
            with start_consumer_span(
                'conversation-consumer-process-message',
                message_data,
                messaging_system='redis',
                message_id=key,
                additional_attributes={
                    'conversation_consumer.event_type': event_type,
                    'conversation_consumer.name': self.consumer_name,
                },
            ):
                # Clean message data for processing (remove trace context)
                clean_data = clean_trace_context_from_message(message_data)
                type_process_mapping[event_type](
                    message_data=clean_data, message_id=message_id
                )

        except Exception as e:
            logger.error(
                f'Error processing conversation message {message_id}: {e}',
                extra={'message_id': message_id, 'key': key},
            )
            # In a production system, you might want to implement retry logic or dead letter queue
            raise

    def _handle_new_conversation_event(
        self, message_id: str, message_data: Dict[str, Any]
    ) -> None:
        """
        Handle NewConversationEvent by starting conversation processing.

        Args:
            event: The NewConversationEvent to handle
            message_id: The message ID for tracking
        """
        event = NewConversationEvent(**message_data)
        try:
            logger.info(
                f'Handling NewConversationEvent for conversation {event.conversation_id}',
                extra={
                    'conversation_id': event.conversation_id,
                    'message_id': message_id,
                },
            )

            conversation_init_data = ConversationInitData(
                **event.conversation_init_data
            )
            if not (event.conversation_id and event.user_id):
                logger.error(
                    f'Invalid NewConversationEvent for {event.conversation_id} and {event.user_id}',
                    extra={
                        'conversation_id': event.conversation_id,
                        'user_id': event.user_id,
                    },
                )
                return

            initial_message_action = None
            if event.initial_user_msg or event.image_urls:
                user_msg = (
                    event.initial_user_msg.format(event.conversation_id)
                    if event.attach_convo_id and event.initial_user_msg
                    else event.initial_user_msg
                )
                initial_message_action = MessageAction(
                    content=user_msg or '',
                    image_urls=event.image_urls or [],
                    mode=event.research_mode,
                )

            # Process the conversation creation in the event loop
            _run_in_loop(
                self._process_conversation_creation(
                    conversation_id=event.conversation_id,
                    conversation_init_data=conversation_init_data,
                    user_id=event.user_id,
                    initial_message_action=initial_message_action,
                    replay_json=event.replay_json,
                    system_prompt=event.system_prompt,
                    user_prompt=event.user_prompt,
                    github_user_id=event.github_user_id,
                    mnemonic=event.mnemonic,
                    mcp_disable=event.mcp_disable,
                    knowledge_base=event.knowledge_base,
                    space_id=event.space_id,
                    thread_follow_up=event.thread_follow_up,
                    research_mode=event.research_mode,
                    raw_followup_conversation_id=event.raw_followup_conversation_id,
                    space_section_id=event.space_section_id,
                    output_config=event.output_config,
                ),
                loop=self.loop,
                timeout=100,
                is_waiting=False,
            )

        except Exception as e:
            logger.error(
                f'Error handling NewConversationEvent for {event.conversation_id}: {e}',
                extra={
                    'conversation_id': event.conversation_id,
                    'message_id': message_id,
                },
            )

    async def _process_conversation_creation(
        self,
        conversation_id: str,
        conversation_init_data: ConversationInitData,
        user_id: str,
        initial_message_action: MessageAction | None,
        replay_json: str | None,
        system_prompt: str | None,
        user_prompt: str | None,
        github_user_id: str | None,
        mnemonic: str | None,
        mcp_disable: dict[str, bool] | None,
        knowledge_base: list[dict] | None,
        space_id: int | None,
        thread_follow_up: int | None,
        research_mode: str | None,
        raw_followup_conversation_id: str | None,
        space_section_id: int | None,
        output_config: dict | None,
    ) -> None:
        """
        Process conversation creation using the conversation manager.

        This method runs in the asyncio event loop to properly handle
        the async conversation manager methods and publish events.
        """
        try:
            session_init_args = await settings_for_conversation(
                user_id,
                conversation_init_data.git_provider_tokens,
                conversation_init_data.selected_repository,
                conversation_init_data.selected_branch,
            )
            conversation_settings_init_data = ConversationInitData(**session_init_args)

            logger.info(
                f'Starting conversation {conversation_id} via conversation manager',
                extra={'conversation_id': conversation_id, 'user_id': user_id},
            )

            # Use the conversation manager to start the agent loop
            await self.conversation_manager.maybe_start_agent_loop(
                sid=conversation_id,
                settings=conversation_settings_init_data,
                user_id=user_id,
                initial_user_msg=initial_message_action,
                replay_json=replay_json,
                github_user_id=github_user_id,
                mnemonic=mnemonic,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                mcp_disable=mcp_disable,
                knowledge_base=knowledge_base,
                space_id=space_id,
                thread_follow_up=thread_follow_up,
                research_mode=research_mode,
                raw_followup_conversation_id=raw_followup_conversation_id,
                space_section_id=space_section_id,
                output_config=output_config,
                is_start_agent=True,
            )
            await self._subscribe_forwarder(
                conversation_id=conversation_id, user_id=user_id
            )
        except Exception as e:
            logger.error(
                f'Error in conversation creation for {conversation_id}: {e}',
                extra={'conversation_id': conversation_id},
            )
            # Publish error event

    async def _subscribe_forwarder(self, conversation_id: str, user_id: str):
        """Subscribe to the event stream for the conversation."""
        conversation = await self.conversation_manager.attach_to_conversation(
            sid=conversation_id, user_id=user_id
        )

        if not (conversation and conversation.event_stream):
            logger.warning(
                f'No event stream available for conversation {conversation_id}',
                extra={'conversation_id': conversation_id},
            )
            return

        logger.info(
            f'Successfully started conversation {conversation_id}',
            extra={'conversation_id': conversation_id},
        )

        # Publish conversation started event
        if not hasattr(conversation.event_stream, 'subscribe'):
            logger.error(
                f'Failed to start conversation {conversation_id}',
                extra={'conversation_id': conversation_id},
            )
            return
        try:
            conversation.event_stream.subscribe(
                subscriber_id=EventStreamSubscriber.CONVERSATION_CONSUMER,
                callback=self._create_event_forwarder(conversation_id, user_id),
                callback_id=f'worker_forwarder_{conversation_id}',
            )
        except ValueError:
            pass  # Already subscribed - take no action

    def _create_event_forwarder(self, conversation_id: str, user_id: str):
        """Create an event forwarder function for the conversation."""

        def forward_event(event, *args, **kwargs):
            """Forward events from worker to message queue."""
            try:
                # Convert event to dictionary for publishing
                if isinstance(event, Event):
                    event_data = event_to_dict(event)
                else:
                    event_data = event.model_dump()

                self._publish_process_conversation_event(
                    conversation_id=conversation_id,
                    user_id=user_id,
                    event_data=event_data,
                    source=self.consumer_name,
                )
            except Exception as e:
                logger.error(f'Error forwarding event for {conversation_id}: {e}')

        return forward_event

    def _publish_process_conversation_event(
        self,
        conversation_id: str,
        user_id: str,
        event_data: Dict[str, Any],
        source: str,
        **kwargs,
    ) -> None:
        """Publish a ProcessConversationEvent to the message queue."""
        if not self.publisher:
            logger.error('Publisher not available')
            return

        try:
            with start_producer_span(
                'conversation-consumer-publish-event',
                messaging_system='redis',
                destination='process_conversation_events',
                message_id=conversation_id,
                additional_attributes={
                    'conversation_consumer.conversation_id': conversation_id,
                    'conversation_consumer.source': source,
                },
            ) as span:
                process_event = create_process_conversation_event(
                    conversation_id=conversation_id,
                    user_id=user_id,
                    event_data=event_data,
                    source=source,
                    **kwargs,
                )

                message_id = self.publisher.publish_message(
                    key=conversation_id, data=process_event.model_dump()
                )

                span.set_attribute('messaging.redis.message_id', message_id)

                logger.debug(
                    f'Published ProcessConversationEvent for {conversation_id}: {message_id}',
                    extra={'conversation_id': conversation_id},
                )

        except Exception as e:
            logger.error(
                f'Error publishing ProcessConversationEvent for {conversation_id}: {e}'
            )

    def _handle_user_action_event(
        self, message_id: str, message_data: Dict[str, Any]
    ) -> None:
        """Handle UserActionEvent by forwarding to the conversation manager."""
        event = UserActionEvent(**message_data)
        try:
            _run_in_loop(
                self._process_user_action_event(
                    conversation_id=event.conversation_id,
                    user_id=event.user_id,
                    event_data=event.event_data,
                ),
                loop=self.loop,
                timeout=100,
                is_waiting=False,
            )

        except Exception as e:
            logger.error(
                f'Error handling  for {event.conversation_id}: {e}',
                extra={
                    'conversation_id': event.conversation_id,
                    'message_id': message_id,
                },
            )

    async def _process_user_action_event(
        self, conversation_id: str, user_id: str, event_data: Dict[str, Any]
    ) -> None:
        try:
            if not await self.conversation_manager.is_agent_loop_running(
                conversation_id
            ):
                settings = await get_user_setting(user_id)
                await self.conversation_manager.maybe_start_agent_loop(
                    sid=conversation_id, settings=settings, user_id=user_id
                )
                await self._subscribe_forwarder(
                    conversation_id=conversation_id, user_id=user_id
                )
            if event_data:
                await self.conversation_manager.send_to_event_stream_by_sid(
                    conversation_id, data=event_data
                )
        except Exception as e:
            logger.error(
                f'Error processing user action event for {conversation_id}: {e}',
                extra={'conversation_id': conversation_id},
            )


def main():
    """Main function to run the conversation consumer."""

    consumer_name = 'conversation_worker_{}'.format(time.time())

    from openhands.server.shared import conversation_manager
    from openhands.shared import config as app_config

    if os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT'):
        from opentelemetry.instrumentation.asyncio import AsyncioInstrumentor
        from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
        from opentelemetry.instrumentation.psycopg import PsycopgInstrumentor
        from opentelemetry.instrumentation.threading import ThreadingInstrumentor
        from traceloop.sdk import Traceloop

        AsyncioInstrumentor().instrument()
        ThreadingInstrumentor().instrument()
        PsycopgInstrumentor().instrument()
        AsyncPGInstrumentor().instrument()

        if os.getenv('TRACELOOP_BASE_URL'):
            Traceloop.init(
                disable_batch=False,
                app_name=os.getenv('OTEL_SERVICE_NAME', 'openhands'),
            )

    # Create and run the consumer
    message_consumer = create_consumer_from_config(
        consumer_name=consumer_name,
        group_name='conversation_processing_consumers',
        config=app_config,
        read_from_start=False,
    )
    publisher = create_publisher_from_config(
        publisher_name=consumer_name,
        config=app_config,
    )

    # Get shutdown timeout from environment or use default
    shutdown_timeout = int(os.getenv('CONVERSATION_CONSUMER_SHUTDOWN_TIMEOUT', '300'))

    consumer = ConversationConsumer(
        consumer_name=consumer_name,
        message_consumer=message_consumer,
        publisher=publisher,
        conversation_manager=conversation_manager,
        shutdown_timeout=shutdown_timeout,
    )

    logger.info(f'Starting conversation consumer: {consumer_name}')
    consumer.run()


if __name__ == '__main__':
    main()
