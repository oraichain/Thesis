import asyncio
import json
import os
import uuid
from abc import ABC
from typing import AsyncGenerator, List

import httpx
from a2a.client import A2ACardResolver, A2AClient
from a2a.client.errors import A2AClientHTTPError, A2AClientJSONError
from a2a.types import (
    AgentCard,
    Message,
    MessageSendParams,
    SendMessageRequest,
    SendMessageResponse,
    SendStreamingMessageResponse,
    TextPart,
)

from openhands.core.logger import openhands_logger as logger

A2A_REQUEST_DEFAULT_TIMEOUT = float(os.getenv('A2A_REQUEST_DEFAULT_TIMEOUT') or 120.0)


class A2AManager(ABC):
    list_remote_agent_servers: List[str] = []
    list_remote_agent_cards: dict[str, AgentCard] = {}

    def __init__(self, a2a_server_urls: List[str]):
        self.list_remote_agent_servers = a2a_server_urls
        self.list_remote_agent_cards = {}

    def register_remote_card(self, agent_card: AgentCard):
        self.list_remote_agent_cards[agent_card.name] = agent_card

    async def initialize_agent_cards(self):
        if not self.list_remote_agent_servers:
            return

        async def fetch_card(server_url: str) -> AgentCard | None:
            async with httpx.AsyncClient() as httpx_client:
                resolver = A2ACardResolver(httpx_client, server_url)
                try:
                    return await resolver.get_agent_card()
                except (A2AClientHTTPError, A2AClientJSONError) as e:
                    logger.error(
                        f'Failed to fetch agent card from {server_url}: {str(e)}'
                    )
                    return None

        tasks = [fetch_card(server) for server in self.list_remote_agent_servers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for card in results:
            if isinstance(card, AgentCard):
                logger.info(f'Registered remote agent card: {card.name}')
                self.list_remote_agent_cards[card.name] = card

    def list_remote_agents(self):
        """List the available remote agents you can use to delegate the task."""
        if not self.list_remote_agent_cards:
            return []
        remote_agent_info = []
        for card in self.list_remote_agent_cards.values():
            remote_agent_info.append(
                {
                    'agent_name': card.name,
                    'agent_description': card.description,
                    'agent_skills': json.dumps(
                        [
                            {
                                'skill_name': skill.name,
                                'skill_description': skill.description,
                                'skill_examples': skill.examples,
                            }
                            for skill in card.skills
                        ]
                    ),
                }
            )
        return remote_agent_info

    async def send_message(
        self, agent_name: str, message: str, sid: str, role: str = 'user'
    ) -> AsyncGenerator[SendStreamingMessageResponse | SendMessageResponse, None]:
        """Send a task to a remote agent and yield task responses.

        Args:
            agent_name: Name of the remote agent
            message: Message to send to the agent
            sid: Session ID

        Yields:
            TaskStatusUpdateEvent or Task: Task response updates
        """
        if agent_name not in self.list_remote_agent_cards:
            raise ValueError(f'Agent {agent_name} not found')

        card = self.list_remote_agent_cards[agent_name]
        async with httpx.AsyncClient(
            timeout=A2A_REQUEST_DEFAULT_TIMEOUT
        ) as httpx_client:
            client = A2AClient(httpx_client, card)
            params: MessageSendParams = MessageSendParams(
                message=Message(
                    role=role,
                    parts=[TextPart(text=message)],
                    message_id=uuid.uuid4().hex,
                ),
                acceptedOutputModes=['text', 'text/plain', 'image/png'],
                metadata={'conversation_id': sid, 'session_id': sid},
            )
            request: SendMessageRequest = SendMessageRequest(
                id=str(uuid.uuid4()),
                params=params,
            )

            logger.info(f'Sending task to {agent_name} with message: {message}')
            logger.info(f'Card capabilities: {card.capabilities}')
            if card.capabilities.streaming:
                async for response in client.send_message_streaming(request=request):
                    yield response
            else:
                response = await client.send_message(request=request)
                yield response

    # async def send_cancel_task(self, task_id: str, sid: str):
    #     pass

    @classmethod
    def from_toml_config(cls, config: dict) -> 'A2AManager':
        a2a_manager = cls(config['a2a_server_url'])
        return a2a_manager
