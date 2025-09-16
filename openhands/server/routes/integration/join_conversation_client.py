import json
import queue
import threading
import time
import urllib.parse  # Added for URL encoding
from typing import Generator

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
        self.sio = socketio.Client(reconnection_attempts=5, reconnection_delay=2)
        self.message_queue: queue.Queue = queue.Queue()
        self.connected = False
        self.finished = False
        self.client_disconnected = False
        self.agent_ready = False  # Track if agent is ready to process actions
        self.cancel_event = threading.Event()  # Cancellation signal
        self.action_lock = (
            threading.Lock()
        )  # Added lock for agent_ready and action_queue
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
        def connect():
            connection_event = {
                'type': 'connection',
                'status': 'connected',
                'message': 'Connected to conversation',
            }
            try:
                self._safe_queue_put(connection_event)
            except Exception as e:
                logger.error(f'Failed to queue connect event: {e}')

        @self.sio.event
        def oh_event(data):
            try:
                # Check if this is an agent ready event
                with self.action_lock:  # Synchronize access
                    if (
                        isinstance(data, dict)
                        and data.get('observation') == 'agent_ready'
                    ):
                        self.agent_ready = True
                        self.sio.emit('oh_user_action', self.action)
                # Stream the complete event data. Only stream if agent is ready to get new data.
                if self.agent_ready:
                    complete_event = {
                        'type': 'oh_event',
                        'data': data,
                    }
                    self._safe_queue_put(complete_event)
            except Exception as e:
                logger.error(f'Error in oh_event handler: {e}')

    def connect(
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
            self.sio.connect(
                f'{api_base_url}?{query_string}',
                socketio_path='/socket.io',
                transports=['websocket'],
                namespaces='/',
                wait_timeout=5,
            )
            self.connected = True
            self.finished = False
        except socketio.exceptions.ConnectionError as e:
            logger.error(f'Connection failed: {e}')
            raise  # Re-raise to ensure caller is notified immediately

    def _safe_queue_put(self, item):
        """Safely put item in queue, handling overflow gracefully"""
        self.message_queue.put(item)

    def cancel(self):
        """Signal external cancellation"""
        self.cancel_event.set()  # Wake up any blocking operations

    def _graceful_disconnect(self):
        """Gracefully disconnect and cleanup resources"""
        try:
            self.finished = True
            self.connected = False
            self.agent_ready = False
            self.client_disconnected = True
            # Clear message queue
            while not self.message_queue.empty():
                try:
                    self.message_queue.get_nowait()
                except queue.Empty:
                    break
            if self.sio and self.sio.connected:
                # self.sio.emit("close_session", conversation_id=self.conversation_id)
                self.sio.disconnect()
        except Exception as e:
            logger.error(f'Error during graceful disconnect: {e}')

    def stream(
        self,
        user_prompt: str,
        research_mode: ResearchMode,
        stream_timeout: int = 120,  # Added configurable stream timeout
    ) -> Generator[str, None, None]:
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
            with self.action_lock:  # Synchronize access
                if not self.agent_ready:
                    logger.debug('Agent not ready - buffering action')
                    self.action = action_payload
                else:
                    self.sio.emit('oh_user_action', action_payload)

            # Stream events as they arrive with health monitoring and timeout
            while not self.finished:
                try:
                    # Check if we got cancelled first
                    if self.cancel_event.is_set():
                        self._graceful_disconnect()
                        break

                    # Try to get a message with timeout
                    try:
                        event = self.message_queue.get(timeout=stream_timeout)
                        # Send complete event as JSON
                        event_json = json.dumps(event, cls=self.json_encoder)
                        yield event_json
                        if has_finished_processing_user_action(event):
                            logger.info(
                                'Agent has finished processing user action, disconnecting'
                            )
                            break
                    except queue.Empty:
                        # This means we timed out
                        timeout_event = {
                            'type': 'completion',
                            'status': 'finished',
                            'message': f'No messages received for {stream_timeout} seconds, disconnecting',
                        }
                        yield json.dumps(timeout_event, cls=self.json_encoder)
                        self._graceful_disconnect()
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
            self._graceful_disconnect()
