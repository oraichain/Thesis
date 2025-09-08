"""
Base publisher class for OpenHands worker system.

This module provides the abstract base class for different types of publishers
(Redis, Kafka, etc.) that can be implemented for the OpenHands message queue system.
"""

import abc
import json
import signal
import threading
from typing import Any, Dict


class BasePublisher(abc.ABC):
    """
    Abstract base class for message publishers.

    This class defines the interface and common functionality for different
    types of publishers (Redis, Kafka, etc.). Subclasses must implement
    the abstract methods to provide specific messaging backend functionality.
    """

    def __init__(self, publisher_name: str):
        """
        Initialize the base publisher.

        Args:
            publisher_name: Unique name for this publisher instance
        """
        self.publisher_name = publisher_name

        # Use atomic Event for thread-safe state management instead of lock + boolean
        self._running_event = threading.Event()

        # Set up signal handlers for graceful shutdown
        self._setup_shutdown_handlers()

    # Abstract methods that must be implemented by subclasses

    @abc.abstractmethod
    def connect(self) -> None:
        """Establish connection to the messaging backend."""
        pass

    @abc.abstractmethod
    def disconnect(self) -> None:
        """Close connection to the messaging backend."""
        pass

    @abc.abstractmethod
    def publish_message(self, key: str, data: Dict[str, Any]) -> str:
        """
        Publish a message to the messaging backend.

        Args:
            key: Message key (used for routing/partitioning by implementations)
            data: Message data to publish

        Returns:
            Message ID or confirmation from the backend
        """
        pass

    # Common functionality implemented in base class

    def _setup_shutdown_handlers(self) -> None:
        """Set up signal handlers for graceful shutdown."""

        def handler(signum, frame):
            print(f'Shutting down publisher {self.publisher_name}')
            self.stop()
            exit(0)

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

    def serialize_data(self, data: Dict[str, Any]) -> str:
        """
        Serialize message data to JSON string.

        Args:
            data: Data to serialize

        Returns:
            JSON string representation
        """
        return json.dumps(data)

    def prepare_message(self, key: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare a message for publishing by adding metadata and serializing.

        Args:
            key: Message key
            data: Message data

        Returns:
            Prepared message dictionary
        """
        message = {'key': str(key), 'data': self.serialize_data(data)}

        return message

    def start(self) -> None:
        """Start the publisher."""
        print(f'🚀 Starting publisher: {self.publisher_name}')

        # Connect to messaging backend
        self.connect()

        # Set running state atomically
        self._running_event.set()
        print(f'Publisher {self.publisher_name} started successfully')

    def stop(self) -> None:
        """Stop the publisher gracefully."""
        print(f'Stopping publisher {self.publisher_name}')

        # Clear running state atomically
        self._running_event.clear()

        try:
            self.disconnect()
        except Exception as e:
            print(f'Error disconnecting: {e}')

    def publish(self, key: str, data: Dict[str, Any]) -> str:
        """
        Publish a message with proper error handling and logging.

        Args:
            key: Message key
            data: Message data

        Returns:
            Message ID from the backend

        Raises:
            Exception: If publishing fails
        """
        # Atomic check of running state - no lock needed
        if not self._running_event.is_set():
            raise RuntimeError(f'Publisher {self.publisher_name} is not running')

        try:
            # Publish the message - backend clients handle thread safety and routing
            message_id = self.publish_message(key, data)

            print(f'Published message with key {key}, ID: {message_id}')

            return message_id

        except Exception as e:
            print(f'Error publishing message with key {key}: {e}')
            raise
