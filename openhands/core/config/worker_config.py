from enum import Enum

from pydantic import BaseModel, Field, ValidationError


class WorkerMode(str, Enum):
    """Enumeration of available worker modes."""

    STANDALONE = 'standalone'
    MULTI_WORKER = 'multi_worker'


class QueueType(str, Enum):
    """Enumeration of available queue types."""

    REDIS = 'redis'


class WorkerConfig(BaseModel):
    """Configuration for the worker system.

    Attributes:
        mode: The worker mode to use. Either "standalone" or "multi_worker".
        queue_type: The type of queue to use for message queuing (required when mode is multi_worker).
        queue_url: The URL of the queue server (optional, can be constructed from host/port).
        queue_host: The hostname of the queue server.
        queue_port: The port number of the queue server.
        queue_db: The database number to use for the queue (optional, defaults to 0).
        queue_password: The password for queue authentication (optional).
        queue_num_partitions: The number of partitions for the queue (optional, defaults to 4).
        queue_max_messages_per_partitions: The maximum number of messages per partition for the queue (optional, defaults to 1000).
    """

    mode: WorkerMode = Field(
        default=WorkerMode.STANDALONE, description='The worker mode to use'
    )
    queue_type: QueueType | None = Field(
        default=None, description='The type of queue to use for message queuing'
    )
    queue_url: str | None = Field(
        default=None, description='The URL of the queue server'
    )
    queue_host: str = Field(
        default='localhost', description='The hostname of the queue server'
    )
    queue_port: int = Field(
        default=6379, description='The port number of the queue server'
    )
    queue_db: int | None = Field(
        default=None, description='The database number to use for the queue'
    )
    queue_password: str | None = Field(
        default=None, description='The password for queue authentication'
    )
    queue_num_partitions: int | None = Field(
        default=None, description='The number of partitions for the queue'
    )
    queue_max_messages_per_partitions: int | None = Field(
        default=None,
        description='The maximum number of messages per partition for the queue',
    )

    model_config = {'extra': 'forbid'}

    def __init__(self, **data):
        super().__init__(**data)
        self._validate_multi_worker_config()

    def _validate_multi_worker_config(self) -> None:
        """Validate configuration when multi_worker mode is selected."""
        if self.mode == WorkerMode.MULTI_WORKER:
            if not self.queue_type:
                raise ValueError("queue_type is required when mode is 'multi_worker'")
            if not self.queue_url and not (self.queue_host and self.queue_port):
                raise ValueError(
                    "Either queue_url or both queue_host and queue_port must be provided when mode is 'multi_worker'"
                )

    @classmethod
    def from_toml_section(cls, data: dict) -> dict[str, 'WorkerConfig']:
        """
        Create a mapping of WorkerConfig instances from a toml dictionary representing the [worker] section.

        The configuration is built from all keys in data.

        Returns:
            dict[str, WorkerConfig]: A mapping where the key "worker" corresponds to the [worker] configuration
        """
        # Initialize the result mapping
        worker_mapping: dict[str, WorkerConfig] = {}

        # Try to create the configuration instance
        try:
            worker_mapping['worker'] = cls.model_validate(data)
        except ValidationError as e:
            raise ValueError(f'Invalid worker configuration: {e}')

        return worker_mapping
