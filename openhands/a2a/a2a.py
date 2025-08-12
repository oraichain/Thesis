# mypy: ignore-errors
import asyncio
from uuid import uuid4

from a2a.client import A2ACardResolver, A2AClient
from a2a.types import AgentCard, JSONRPCErrorResponse, TaskState, TaskStatusUpdateEvent

from containers.runtime.code.openhands.utils.async_utils import call_async_from_sync
from openhands.core.logger import openhands_logger as logger
from openhands.core.message import TextContent


class A2AAgent:
    def __init__(self, a2a_server_url: str, session: str = None, history: bool = False):
        self.session = session
        self.history = history

        self.card_resolver = A2ACardResolver(a2a_server_url)
        self.card: AgentCard = call_async_from_sync(self.card_resolver.get_agent_card())

        self.client = A2AClient(agent_card=self.card)
        if session:
            self.sessionId = session
        else:
            self.sessionId = uuid4().hex

    async def step(self, messages: list[str]):
        continue_loop = True
        streaming = self.card.capabilities.streaming

        while continue_loop:
            taskId = uuid4().hex
            print('=========  starting a new task ======== ')
            continue_loop = await self.completeTask(
                streaming, taskId, self.sessionId, messages
            )

            if self.history and continue_loop:
                print('========= history ======== ')
                task_response = await self.client.get_task(
                    {'id': taskId, 'historyLength': 10}
                )
                print(
                    task_response.model_dump_json(include={'result': {'history': True}})
                )

    async def completeTask(self, streaming, taskId, sessionId, messages: list[str]):
        parts = [
            TextContent(type='text', text=message).serialize_model()
            for message in messages
        ]
        payload = {
            'id': taskId,
            'sessionId': sessionId,
            'acceptedOutputModes': ['text'],
            'message': {
                'role': 'user',
                'parts': parts,
            },
        }

        taskResult = None
        if streaming:
            response_stream = self.client.send_message_streaming(payload)
            async for result in response_stream:
                print(f'stream event => {result.model_dump_json(exclude_none=True)}')
                if (
                    result.root
                    and isinstance(result.root.result, TaskStatusUpdateEvent)
                    and result.root.result.final
                ):
                    return False
        else:
            taskResult = await self.client.send_message(payload)
            print(f'\ntask result => {taskResult.model_dump_json(exclude_none=True)}')
            ## if the result is that more input is required, loop again.
            if isinstance(taskResult.root, JSONRPCErrorResponse):
                logger.error(f'Error sending message to agent A2A: {taskResult.root}')
                return False
            state = TaskState(taskResult.root.result.status.state)
            if state.name == TaskState.input_required.name:
                return await self.completeTask(streaming, taskId, sessionId)
            else:
                ## task is complete
                return False
        return True


if __name__ == '__main__':
    a2a_server_url = 'http://localhost:10000'
    agent = A2AAgent(a2a_server_url=a2a_server_url, history=True)
    asyncio.run(agent.step(['Hello, how are you?']))
