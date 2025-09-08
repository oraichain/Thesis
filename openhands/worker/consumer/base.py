"""
Base consumer class for OpenHands worker system.

This module provides the abstract base class for different types of consumers
(Redis, Kafka, etc.) that can be implemented for the OpenHands message queue system.
"""

import abc
import signal
import time
from typing import Any, Callable, Dict, List, Optional, Tuple


class BaseConsumer(abc.ABC):
    """
    Abstract base class for message consumers.

    This class defines the interface and common functionality for different
    types of consumers (Redis, Kafka, etc.). Subclasses must implement
    the abstract methods to provide specific messaging backend functionality.
    """

    def __init__(
        self,
        consumer_name: str,
        group_name: str = '',
        num_partitions: Optional[int] = None,
        message_processor: Optional[Callable[[str, str, Dict[str, Any]], None]] = None,
        read_from_start: bool = False,
    ):
        """
        Initialize the base consumer.

        Args:
            consumer_name: Unique name for this consumer instance
            group_name: Name of the consumer group
            num_partitions: Number of partitions to use (optional, not needed for Kafka)
            message_processor: Optional callback function for processing messages.
                           If provided, this will be called instead of the default process_message implementation.
                           Signature: (message_id: str, key: str, message_data: Dict[str, Any]) -> None
            read_from_start: If True, consumer will read messages from the beginning of the stream.
                           If False (default), consumer will only read new messages.
        """
        self.consumer_name = consumer_name
        self.group_name = group_name
        self.num_partitions = num_partitions
        self.message_processor = message_processor
        self.read_from_start = read_from_start

        self.assigned_partitions: List[int] = []
        self.is_running = False

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
    def create_consumer_groups(self) -> None:
        """Create consumer groups for all partition streams."""
        pass

    @abc.abstractmethod
    def setup_consumer(self) -> None:
        """Setup the consumer (groups, subscriptions, etc.)."""
        pass

    @abc.abstractmethod
    def cleanup_consumer(self) -> None:
        """Cleanup the consumer resources."""
        pass

    @abc.abstractmethod
    def read_messages(self) -> List[Tuple[str, str, Dict[str, Any]]]:
        """
        Read messages from the consumer.

        Returns:
            List of tuples containing (message_id, key, message_data)
        """
        pass

    @abc.abstractmethod
    def acknowledge_message(self, message_id: str, **kwargs) -> None:
        """
        Acknowledge that a message has been processed.

        Args:
            message_id: ID of the message to acknowledge
            **kwargs: Additional backend-specific parameters
        """
        pass

    @abc.abstractmethod
    def process_message(
        self, message_id: str, key: str, message_data: Dict[str, Any]
    ) -> None:
        """
        Process a single message. Override this method to customize message processing.

        Args:
            message_id: Unique identifier for the message
            key: Message key
            message_data: Message payload
        """
        pass

    # Common functionality implemented in base class

    def _setup_shutdown_handlers(self) -> None:
        """Set up signal handlers for graceful shutdown."""

        def handler(signum, frame):
            print(f'Shutting down consumer {self.consumer_name}')
            self.stop()
            exit(0)

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

    def start(self) -> None:
        """Start the consumer."""
        print(f'🚀 Starting consumer: {self.consumer_name}')
        self.is_running = True

        # Connect to messaging backend
        self.connect()

        # Create consumer groups
        self.create_consumer_groups()

        # Setup consumer (backend-specific setup)
        self.setup_consumer()

        print(f'Consumer {self.consumer_name} started successfully')

    def stop(self) -> None:
        """Stop the consumer gracefully."""
        print(f'Stopping consumer {self.consumer_name}')
        self.is_running = False

        try:
            # Backend-specific cleanup
            self.cleanup_consumer()
        except Exception as e:
            print(f'Error in consumer cleanup: {e}')

        try:
            self.disconnect()
        except Exception as e:
            print(f'Error disconnecting: {e}')

    def run(self) -> None:
        """
        Main consumer loop. Start the consumer and begin processing messages.
        """
        self.start()

        try:
            while self.is_running:
                try:
                    # Read messages
                    messages = self.read_messages()

                    if messages:
                        print(
                            f'Consumer {self.consumer_name} received {len(messages)} messages'
                        )

                        # Process each message
                        for message_id, key, message_data in messages:
                            print(
                                f'Consumer {self.consumer_name} processing message: '
                                f'ID={message_id}, key={key}'
                            )

                            # Process the message
                            self.process_message(message_id, key, message_data)

                            # Acknowledge the message
                            self.acknowledge_message(
                                message_id, key=key, message_data=message_data
                            )

                except Exception as e:
                    print(f'Error processing messages: {e}')
                    time.sleep(1)

                time.sleep(0.01)  # Small delay to prevent tight loop

        except KeyboardInterrupt:
            print(f'Consumer {self.consumer_name} interrupted')
        finally:
            self.stop()
