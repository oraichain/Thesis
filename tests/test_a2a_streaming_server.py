#!/usr/bin/env python3
"""
A2A Streaming Server using A2A Python SDK
Demonstrates proper A2A server implementation with streaming capabilities using AgentExecutor pattern.
"""

import asyncio
import uuid
from typing import Any, AsyncGenerator, Dict

from a2a.server.agent_execution import AgentExecutor
from a2a.server.apps.jsonrpc import A2AFastAPIApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore, TaskUpdater
from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentSkill,
    Message,
    Part,
    TextPart,
    UnsupportedOperationError,
)


class StreamingTokenAgent:
    """A2A Agent that streams tokens for demonstration purposes."""

    async def stream(
        self, query: str, context_id: str, task_id: str
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Stream tokens for the given query."""

        print(f"📨 Received query: '{query}' for task {task_id[:8]}...")
        print('🔄 Starting token streaming...')

        # Stream tokens every second for 10 seconds
        tokens = [
            '🚀 Starting',
            'token',
            'streaming',
            'demo',
            'with',
            'A2A',
            'Python',
            'SDK',
            'AgentExecutor',
            '✨',
        ]

        accumulated_text = ''

        for i, token in enumerate(tokens):
            await asyncio.sleep(1)  # Wait 1 second between tokens

            # Add token to accumulated text
            if accumulated_text:
                accumulated_text += ' '
            accumulated_text += token

            print(f"[Agent] Streaming token {i+1}/10: '{token}'")

            # Yield update with streaming data
            yield {
                'is_task_complete': i == len(tokens) - 1,  # Last token
                'content': accumulated_text,
                'updates': f'Generated token {i+1}/10: {token}',
                'current_token': token,
                'token_number': i + 1,
                'total_tokens': len(tokens),
            }

        print(f'✅ Token streaming completed for task {task_id[:8]}!')


class StreamingTokenAgentExecutor(AgentExecutor):
    """AgentExecutor implementation for the StreamingTokenAgent."""

    def __init__(self):
        self.agent = StreamingTokenAgent()

    async def execute(self, context, event_queue) -> None:
        """Execute the agent task with streaming updates."""

        # Get user input and task context
        query = context.get_user_input()
        # Create task updater for sending updates
        updater = TaskUpdater(event_queue, context.task_id, context.context_id)

        # Create a new task if none exists
        if not context.current_task:
            await updater.submit()

        # Stream responses from the agent
        async for item in self.agent.stream(query, context.context_id, context.task_id):
            is_task_complete = item['is_task_complete']
            content: str = item['content']
            updates: str = item['updates']
            current_token: str = item['current_token']

            if not is_task_complete:
                # Send working status with update message
                await updater.start_work(
                    Message(
                        role='agent',
                        parts=[TextPart(text=updates)],
                        message_id=str(uuid.uuid4()),
                    )
                )

                # Send artifact update with current content
                await updater.add_artifact(
                    [Part(root=TextPart(text=content))],
                    name=f"streaming_output_{item['token_number']}",
                    artifact_id=f"Streaming output - token {item['token_number']}/{item['total_tokens']}",
                    append=True,
                )

                # Store the last token and accumulated content for final artifact
                continue

            # Task is complete - send final artifact as TWO separate add_artifact calls, each with one part

            # Part 1: Last chunked token with distinct ID
            await updater.add_artifact(
                [
                    Part(
                        root=TextPart(
                            text=current_token,
                            metadata={
                                'part_type': 'chunked_content',
                                'part_id': 'chunk_final_token',
                            },
                        )
                    )
                ],
                name='final_streaming_result_chunked_token',
                artifact_id='Final result - chunked token',
                last_chunk=False,
                append=True,
            )

            # Part 2: Complete accumulated content with distinct ID
            await updater.add_artifact(
                [
                    Part(
                        root=TextPart(
                            text=content,
                            metadata={
                                'part_type': 'accumulated_content',
                                'part_id': 'complete_accumulated_text',
                            },
                        )
                    )
                ],
                name='final_streaming_result_accumulated_content',
                artifact_id='Final result - accumulated content',
                last_chunk=True,
                append=False,
            )

            print('[Server] Final artifact created with 2 parts:')
            print(f"  📄 Part 1 (chunk_final_token): '{current_token}'")
            print(f"  📄 Part 2 (complete_accumulated_text): '{content}'")

            # Complete the task
            await updater.complete()
            break

    async def cancel(self, _request_context, _event_queue):
        """Cancel the task (not implemented for this demo)."""
        raise UnsupportedOperationError(
            'StreamingTokenAgentExecutor does not support cancel operation.'
        )


class A2AStreamingServer:
    """A2A Server with streaming capabilities using the A2A Python SDK."""

    def __init__(self):
        # Create agent card with streaming capabilities
        self.agent_card = AgentCard(
            name='Streaming Token Server SDK',
            description='A2A server that demonstrates token streaming using the Python SDK with AgentExecutor',
            version='1.0.0',
            url='http://localhost:8000',
            capabilities=AgentCapabilities(
                streaming=True, pushNotifications=False, stateTransitionHistory=False
            ),
            skills=[
                AgentSkill(
                    id='token_streaming',
                    name='token_streaming',
                    description='Streams tokens every second for demonstration using AgentExecutor pattern',
                    examples=['Stream some tokens', 'Show me streaming demo with SDK'],
                    tags=['streaming', 'demo', 'sdk'],
                )
            ],
            defaultInputModes=['text'],
            defaultOutputModes=['text'],
        )

        # Create core components
        self.task_store = InMemoryTaskStore()
        self.agent_executor = StreamingTokenAgentExecutor()

        # Create request handler with our agent executor
        self.request_handler = DefaultRequestHandler(
            agent_executor=self.agent_executor,
            task_store=self.task_store,
        )

        # Create FastAPI app
        self.app = A2AFastAPIApplication(
            agent_card=self.agent_card, http_handler=self.request_handler
        )

    async def start(self, host: str = 'localhost', port: int = 8000):
        """Start the A2A server."""
        print(
            '🚀 Starting A2A Streaming Server (using A2A Python SDK + AgentExecutor)...'
        )
        print('📡 Server capabilities: streaming=True')
        print(f'🔗 Server URL: http://{host}:{port}')
        print(f'📋 Agent Card: http://{host}:{port}/.well-known/agent.json')
        print('⏱️  Will stream 10 tokens, one per second')
        print('🔧 Using A2A SDK with AgentExecutor pattern')
        print('-' * 70)

        # Get the FastAPI app and set it in the request handler
        app = self.app.build()

        # Start the server using uvicorn
        import uvicorn

        config = uvicorn.Config(app=app, host=host, port=port, log_level='info')
        server = uvicorn.Server(config)

        print(f'✅ Server starting on http://{host}:{port}')
        print('🔄 Ready for streaming requests...')
        print('💡 Test with: python a2a_streaming_client_example.py')
        print('🛑 Press Ctrl+C to stop')

        await server.serve()


async def main():
    """Run the A2A streaming server."""
    server = A2AStreamingServer()
    await server.start()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('\n🛑 Server stopped by user')
    except Exception as e:
        print(f'❌ Server error: {e}')
        import traceback

        traceback.print_exc()
