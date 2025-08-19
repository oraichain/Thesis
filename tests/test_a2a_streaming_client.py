#!/usr/bin/env python3
"""
A2A Streaming Client Example using OpenHands A2A implementation
Demonstrates how to connect to an A2A server and handle streaming responses.
"""

import asyncio
import uuid
from typing import AsyncGenerator

import httpx
from a2a.client import A2ACardResolver, A2AClient
from a2a.types import (
    AgentCard,
    Message,
    MessageSendParams,
    SendMessageRequest,
    SendStreamingMessageResponse,
    TaskArtifactUpdateEvent,
    TaskStatusUpdateEvent,
    TextPart,
)


class A2AStreamingClient:
    """A2A Client that demonstrates streaming capabilities."""

    def __init__(self, server_url: str):
        self.server_url = server_url
        self.card_resolver: A2ACardResolver = None
        self.agent_card: AgentCard = None
        self.client: A2AClient = None

    async def stream(self, message_text: str):
        """Initialize the client by fetching the agent card."""
        print(f'🔗 Connecting to A2A server: {self.server_url}')

        async with httpx.AsyncClient(timeout=120.0) as httpx_client:
            self.card_resolver = A2ACardResolver(httpx_client, base_url=self.server_url)
            try:
                self.agent_card = await self.card_resolver.get_agent_card()
                # Use correct A2AClient initialization pattern (httpx_client as first parameter)
                self.client = A2AClient(httpx_client, self.agent_card)

                print(f'✅ Connected to: {self.agent_card.name}')
                print(f'📋 Description: {self.agent_card.description}')
                print(
                    f'🔧 Streaming supported: {self.agent_card.capabilities.streaming}'
                )
                print(
                    f'🔔 Push notifications: {self.agent_card.capabilities.push_notifications}'
                )

                return await self.display_streaming_tokens(message_text)
            except Exception as e:
                print(f'❌ Failed to connect: {e}')
                return None

    async def send_streaming_message(
        self, message_text: str, session_id: str = None
    ) -> AsyncGenerator[SendStreamingMessageResponse, None]:
        """Send a message and handle streaming response."""

        if not self.client:
            raise RuntimeError('Client not initialized. Call initialize() first.')

        if not self.agent_card.capabilities.streaming:
            raise RuntimeError('Server does not support streaming')

        # Create session ID if not provided
        if not session_id:
            session_id = uuid.uuid4().hex

        # Create message
        message = Message(
            role='user',
            parts=[TextPart(text=message_text)],
            message_id=uuid.uuid4().hex,
        )

        # Create request parameters
        params = MessageSendParams(
            message=message,
            acceptedOutputModes=['text', 'text/plain'],
            metadata={'session_id': session_id},
        )

        request = SendMessageRequest(id=str(uuid.uuid4()), params=params)

        print(f"📤 Sending message: '{message_text}'")
        print('🔄 Starting streaming response...')
        print('-' * 60)

        # Send streaming request and yield responses with proper cleanup
        stream = None
        try:
            stream = self.client.send_message_streaming(request=request)
            async for response in stream:
                print(f'🔍 Raw response: {response.model_dump_json(exclude_none=True)}')
                yield response
        except Exception as e:
            print(f'❌ Streaming request failed: {e}')
            import traceback

            traceback.print_exc()
            raise
        finally:
            # Ensure the stream is properly closed
            if stream is not None:
                try:
                    await stream.aclose()
                except (GeneratorExit, StopAsyncIteration):
                    # These are expected during normal cleanup
                    pass
                except Exception as cleanup_error:
                    print(f'⚠️ Stream cleanup warning: {cleanup_error}')

    async def display_streaming_tokens(self, message_text: str):
        """Display streaming response with real-time token output."""

        token_count = 0
        current_artifact_text = ''

        stream_generator = None
        try:
            stream_generator = self.send_streaming_message(message_text)
            async for response in stream_generator:
                # Print raw response for debugging
                print(f'🔍 Raw response: {response.model_dump_json(exclude_none=True)}')

                # Handle different response types
                if response.root and response.root.result:
                    result = response.root.result

                    # Task status updates
                    if isinstance(result, TaskStatusUpdateEvent):
                        print(f'📊 Task Status: {result.status.state.value}')

                        if result.status.message:
                            print(
                                f'💬 Agent message: {result.status.message.parts[0].root.text}'
                            )

                        if result.final:
                            print('✅ Task completed!')
                            break

                    # Task artifact updates (streaming content)
                    elif isinstance(result, TaskArtifactUpdateEvent):
                        artifact = result.artifact

                        # Extract text content
                        new_text = ''
                        for part in artifact.parts:
                            if isinstance(part.root, TextPart):
                                new_text = part.root.text
                                break

                        # Display new tokens
                        if new_text != current_artifact_text:
                            if result.append and current_artifact_text:
                                # Show only the new part
                                new_part = new_text[
                                    len(current_artifact_text) :
                                ].strip()
                                if new_part:
                                    token_count += 1
                                    print(f'🔥 Token {token_count}: {new_part}')
                            else:
                                # Show full content
                                token_count += 1
                                print(f'🔥 Token {token_count}: {new_text}')

                            current_artifact_text = new_text

                        if result.last_chunk:
                            print('📄 Final artifact received!')
                            print(f'📝 Complete text: {current_artifact_text}')

                    # Regular task response
                    elif hasattr(result, 'status'):
                        print(
                            f'📋 Task: {result.id[:8]}... Status: {result.status.state.value}'
                        )

                        if result.artifacts:
                            for artifact in result.artifacts:
                                print(f'📄 Artifact: {artifact.name}')

                                # Handle multiple parts with distinct IDs
                                for i, part in enumerate(artifact.parts):
                                    if hasattr(part, 'root') and hasattr(
                                        part.root, 'text'
                                    ):
                                        # Check if part.root (TextPart) has metadata with IDs
                                        part_info = ''
                                        if (
                                            hasattr(part.root, 'metadata')
                                            and part.root.metadata
                                        ):
                                            part_type = part.root.metadata.get(
                                                'part_type', f'part_{i+1}'
                                            )
                                            part_id = part.root.metadata.get(
                                                'part_id', f'id_{i+1}'
                                            )
                                            part_info = f' [{part_type}:{part_id}]'
                                        else:
                                            part_info = f' [part_{i+1}]'

                                        print(
                                            f'📝 Content{part_info}: {part.root.text}'
                                        )
                                    elif isinstance(part.root, TextPart):
                                        # Fallback for direct text parts
                                        print(
                                            f'📝 Content [part_{i+1}]: {part.root.text}'
                                        )

        except Exception as e:
            print(f'❌ Streaming error: {e}')
            return
        finally:
            # Properly close the async generator to avoid cleanup errors
            if stream_generator is not None:
                try:
                    await stream_generator.aclose()
                except GeneratorExit:
                    # Expected when the generator is already closed
                    pass
                except Exception as cleanup_error:
                    print(f'⚠️ Generator cleanup warning: {cleanup_error}')

        print('-' * 60)
        print(f'🎉 Received {token_count} tokens total')


async def main():
    """Run the A2A streaming client example."""

    # You can change this to any A2A server URL
    server_url = 'http://localhost:8000'  # Mock server URL

    print('🚀 A2A Streaming Client Example')
    print('=' * 50)

    client = A2AStreamingClient(server_url)

    # Send a message that might trigger streaming
    message = 'Please provide a detailed response with multiple parts'

    await client.stream(message)

    print('👋 Client finished!')


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('\n🛑 Client stopped by user')
    except Exception as e:
        print(f'❌ Client error: {e}')
