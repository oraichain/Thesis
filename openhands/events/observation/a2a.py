from dataclasses import dataclass

from a2a.types import (
    Message,
    Task,
    TaskArtifactUpdateEvent,
    TaskStatusUpdateEvent,
)

from openhands.core.schema import ObservationType
from openhands.events.observation.observation import Observation


@dataclass
class A2ASendTaskUpdateObservation(Observation):
    """This data class represents the result of a A2A Send Task operation."""

    agent_name: str
    task_update_event: TaskStatusUpdateEvent
    observation: str = ObservationType.A2A_SEND_TASK_UPDATE_EVENT

    @property
    def message(self) -> str:
        return self.content


@dataclass
class A2ASendTaskArtifactObservation(Observation):
    """This data class represents the result of a A2A Send Task operation."""

    agent_name: str
    task_artifact_event: TaskArtifactUpdateEvent
    observation: str = ObservationType.A2A_SEND_TASK_ARTIFACT

    @property
    def message(self) -> str:
        return self.content


@dataclass
class A2ASendTaskResponseObservation(Observation):
    """This data class represents the result of a A2A Send Task operation."""

    agent_name: str
    task: Task
    observation: str = ObservationType.A2A_SEND_TASK_RESPONSE

    @property
    def message(self) -> str:
        return self.content


@dataclass
class A2ASendMessageResponseObservation(Observation):
    """This data class represents the result of a A2A Send Message operation."""

    agent_name: str
    resp_message: Message
    observation: str = ObservationType.A2A_SEND_MESSAGE_RESPONSE

    @property
    def message(self) -> str:
        return self.content
