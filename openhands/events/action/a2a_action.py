from dataclasses import dataclass

from openhands.core.schema.action import ActionType
from openhands.events.action.action import Action


@dataclass
class A2ASendTaskAction(Action):
    agent_name: str
    task_message: str
    action: str = ActionType.A2A_SEND_TASK

    @property
    def message(self) -> str:
        return f"""I am sending a task to the remote agent {self.agent_name} with: \n
            task_message: \n {self.task_message} \n
          """

    def __str__(self) -> str:
        return f"""A2ASendTaskAction(
            agent_name={self.agent_name},
            task_message={self.task_message},
        )"""
