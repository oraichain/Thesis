from a2a.types import TaskState, TaskStatusUpdateEvent, TextPart

from openhands.core.message import TextContent
from openhands.events.observation.a2a import A2ASendTaskUpdateObservation


class TaskEventHandler:
    @staticmethod
    def should_step_on_task_update(event: A2ASendTaskUpdateObservation) -> bool:
        task_update_event = TaskStatusUpdateEvent(**event.task_update_event)
        if task_update_event.final:
            return True
        state = task_update_event.status.state
        match state:
            case TaskState.submitted:
                return False
            case TaskState.working:
                return False
            case TaskState.completed:
                return False
            case TaskState.unknown:
                return False
            case TaskState.input_required:
                return False
            case TaskState.canceled:
                return False
            case TaskState.failed:
                return True
        raise ValueError(f'Unknown task state: {state}')

    @staticmethod
    def handle_observation(
        event: A2ASendTaskUpdateObservation,
    ) -> list[TextContent] | None:
        task_update_event = TaskStatusUpdateEvent(**event.task_update_event)
        if task_update_event.final:
            return None
        state = task_update_event.status.state
        match state:
            case TaskState.submitted:
                return None
            case TaskState.working:
                return None
            case TaskState.completed:
                return None
            case TaskState.unknown:
                return None
            case TaskState.canceled:
                return None
            case TaskState.failed:
                return None
            case TaskState.input_required:
                if task_update_event.status.message:
                    return [
                        TextContent(
                            text=f'Agent {event.agent_name} is waiting for input'
                        ),
                        *[
                            TextContent(text=part.root.text)
                            for part in task_update_event.status.message.parts
                            if isinstance(part.root, TextPart)
                        ],
                    ]
                return None

        raise ValueError(f'Unknown task state: {state}')
