import asyncio
import json
import time
import urllib.parse  # Added for URL encoding
from typing import AsyncGenerator

import socketio
import socketio.exceptions

from openhands.core.logger import openhands_logger as logger
from openhands.core.schema.agent import AgentState
from openhands.core.schema.research import ResearchMode


def has_finished_processing_user_action(event: dict) -> bool:
    """Handle different types of events from the stream"""
    event_type = event.get('type', 'unknown')

    if event_type == 'oh_event':
        # This is the main socket.io event data
        data = event.get('data', {})
        if 'extras' in data and data['extras']:
            extras = data['extras']
            if 'agent_state' in extras:
                if (
                    extras['agent_state'] == AgentState.AWAITING_USER_INPUT
                    or extras['agent_state'] == AgentState.FINISHED
                ):
                    return True
    return False


class SocketStreamClient:
    def __init__(self):
        self.sio = socketio.AsyncClient(reconnection_attempts=5, reconnection_delay=2)
        self.message_queue: asyncio.Queue = asyncio.Queue()
        self.connected = False
        self.finished = False
        self.client_disconnected = False
        self.agent_ready = False  # Track if agent is ready to process actions
        self.cancel_event = asyncio.Event()  # Cancellation signal
        self.action_lock = asyncio.Lock()  # Added lock for agent_ready and action_queue
        self.action: dict | None = None

        # Custom JSON encoder for non-serializable objects
        class CustomJSONEncoder(json.JSONEncoder):
            def default(self, obj):
                try:
                    return str(obj)
                except Exception:
                    return json.JSONEncoder.default(self, obj)

        self.json_encoder = CustomJSONEncoder

        # Set up event handlers before connecting
        @self.sio.event
        async def connect():
            connection_event = {
                'type': 'connection',
                'status': 'connected',
                'message': 'Connected to conversation',
            }
            try:
                await self._safe_queue_put(connection_event)
            except Exception as e:
                logger.error(f'Failed to queue connect event: {e}')

        @self.sio.event
        async def oh_event(data):
            try:
                # Check if this is an agent ready event
                async with self.action_lock:  # Synchronize access
                    if (
                        isinstance(data, dict)
                        and data.get('observation') == 'agent_ready'
                    ):
                        self.agent_ready = True
                        await self.sio.emit('oh_user_action', self.action)
                # Stream the complete event data. Only stream if agent is ready to get new data.
                if self.agent_ready:
                    complete_event = {
                        'type': 'oh_event',
                        'data': data,
                    }
                    await self._safe_queue_put(complete_event)
            except Exception as e:
                logger.error(f'Error in oh_event handler: {e}')

    async def connect(
        self,
        conversation_id: str,
        api_base_url: str,
        research_mode: ResearchMode,
        latest_event_id: int | None = None,
        x_device_id: str | None = None,
        jwt_token: str | None = None,
        api_key: str | None = None,
    ):
        """Connect to socket and yield messages as they arrive"""
        # Validate api_base_url
        if not api_base_url.startswith(('http://', 'https://')):
            raise ValueError('api_base_url must start with http:// or https://')

        # URL-encode query parameters
        query_params = {
            'conversation_id': conversation_id,
            'research_mode': research_mode.value,
            'mode': 'normal',
        }
        if latest_event_id:
            query_params['latest_event_id'] = str(latest_event_id)
        if x_device_id:
            query_params['x-device-id'] = x_device_id
        if jwt_token:
            query_params['auth'] = jwt_token
        elif api_key:
            query_params['api_key'] = api_key
        else:
            raise ValueError('No authentication provided')

        query_string = urllib.parse.urlencode(query_params)

        try:
            await self.sio.connect(
                f'{api_base_url}?{query_string}',
                transports=['websocket'],
                wait_timeout=5,
                retry=True,
            )
            self.connected = True
            self.finished = False
        except socketio.exceptions.ConnectionError as e:
            logger.error(f'Connection failed: {e}')
            raise  # Re-raise to ensure caller is notified immediately

    async def _safe_queue_put(self, item):
        """Safely put item in queue, handling overflow gracefully"""
        await self.message_queue.put(item)

    def cancel(self):
        """Signal external cancellation"""
        self.cancel_event.set()  # Wake up any blocking operations

    async def _graceful_disconnect(self):
        """Gracefully disconnect and cleanup resources"""
        try:
            self.finished = True
            self.connected = False
            self.agent_ready = False
            self.client_disconnected = True
            # Clear message queue
            while not self.message_queue.empty():
                self.message_queue.get_nowait()
            if self.sio and self.sio.connected:
                # await self.sio.emit("close_session", conversation_id=self.conversation_id)
                await self.sio.disconnect()
        except Exception as e:
            logger.error(f'Error during graceful disconnect: {e}')

    async def stream(
        self,
        user_prompt: str,
        research_mode: ResearchMode,
        stream_timeout: int = 120,  # Added configurable stream timeout
    ) -> AsyncGenerator[str, None]:
        """Connect to socket and yield messages as they arrive"""
        try:
            action_payload = {
                'action': 'message',
                'args': {
                    'content': user_prompt,
                    'timestamp': time.time(),
                    'mode': research_mode,
                },
            }

            # Buffer the action if agent is not ready, otherwise send immediately
            async with self.action_lock:  # Synchronize access
                if not self.agent_ready:
                    logger.debug('Agent not ready - buffering action')
                    self.action = action_payload
                else:
                    await self.sio.emit('oh_user_action', action_payload)

            # Stream events as they arrive with health monitoring and timeout
            while not self.finished:
                try:
                    # Wait for either a message or cancellation signal with configurable timeout
                    queue_task = asyncio.create_task(self.message_queue.get())
                    cancel_task = asyncio.create_task(self.cancel_event.wait())

                    done, pending = await asyncio.wait(
                        [queue_task, cancel_task],
                        timeout=stream_timeout,
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    # Cancel any pending tasks
                    for task in pending:
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass

                    # Check if we got cancelled
                    if cancel_task in done:
                        await self._graceful_disconnect()
                        break

                    # Check if we got a message
                    if queue_task in done:
                        event = await queue_task
                        # Send complete event as JSON
                        event_json = json.dumps(event, cls=self.json_encoder)
                        yield event_json
                        if has_finished_processing_user_action(event):
                            logger.info(
                                'Agent has finished processing user action, disconnecting'
                            )
                            break
                    else:
                        # This means we timed out
                        raise asyncio.TimeoutError()

                except asyncio.TimeoutError:
                    # Configurable timeout reached - disconnect and exit
                    timeout_event = {
                        'type': 'completion',
                        'status': 'finished',
                        'message': f'No messages received for {stream_timeout} seconds, disconnecting',
                    }
                    yield json.dumps(timeout_event, cls=self.json_encoder)
                    await self._graceful_disconnect()
                    break
                except json.JSONDecodeError as e:
                    error_event = {
                        'type': 'error',
                        'error': 'JSONDecodeError',
                        'message': f'Failed to serialize event: {e}',
                    }
                    yield json.dumps(error_event, cls=self.json_encoder)
                    break
                except Exception as e:
                    error_event = {
                        'type': 'error',
                        'error': 'Exception',
                        'message': f'Unexpected error: {e}',
                    }
                    yield json.dumps(error_event, cls=self.json_encoder)
                    break

        except socketio.exceptions.ConnectionError as e:
            error_event = {
                'type': 'error',
                'error': 'ConnectionError',
                'message': f'SocketIO connection error: {e}',
            }
            yield json.dumps(error_event, cls=self.json_encoder)
        except Exception as e:
            error_event = {
                'type': 'error',
                'error': 'Exception',
                'message': f'Unexpected error: {e}',
            }
            yield json.dumps(error_event, cls=self.json_encoder)
        finally:
            # Always attempt graceful cleanup
            await self._graceful_disconnect()
