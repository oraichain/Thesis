#!/usr/bin/env python3
"""
Simple FastAPI client that calls the /join-conversation-stream endpoint
and prints responses from the server in real-time.
"""

import asyncio
import json
import os
import sys

import httpx

from openhands.core.schema.research import ResearchMode


class StreamingClient:
    def __init__(self, base_url: str = 'http://localhost:3000'):
        self.base_url = base_url
        self.client = None

    async def __aenter__(self):
        self.client = httpx.AsyncClient(timeout=300.0)  # 5 minute timeout
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.aclose()

    async def _handle_event(self, event: dict):
        """Handle different types of events from the stream"""
        event_type = event.get('type', 'unknown')

        if event_type == 'connection':
            status = event.get('status', '')
            message = event.get('message', '')
            if status == 'connected':
                print(f'🔗 {message}')
            elif status == 'disconnected':
                print(f'🔌 {message}')

        elif event_type == 'oh_event':
            # This is the main socket.io event data
            data = event.get('data', {})
            print('\n📨 Socket Event:')
            print(f"   Type: {data.get('type', 'N/A')}")
            print(f"   Source: {data.get('source', 'N/A')}")
            if 'content' in data:
                print(f"   Content: {data['content']}")
            if 'message' in data:
                print(f"   Message: {data['message']}")
            if 'observation' in data:
                print(f"   Observation: {data['observation']}")
            if 'extras' in data and data['extras']:
                extras = data['extras']
                if 'agent_state' in extras:
                    print(f"   Agent State: {extras['agent_state']}")
                    if extras['agent_state'] == 'awaiting_user_input':
                        print(
                            '   🏁 Agent is now awaiting user input - conversation completed!'
                        )
            # Print full event data for debugging
            print(f'   Full Data: {json.dumps(data, indent=2)}')
            print('-' * 30)

        elif event_type == 'heartbeat':
            print('💓 Heartbeat', end='', flush=True)

        elif event_type == 'error':
            error_type = event.get('error', 'Unknown')
            message = event.get('message', '')
            print(f'\n❌ Error ({error_type}): {message}')

        elif event_type == 'completion':
            reason = event.get('reason', 'unknown')
            status = event.get('status', 'finished')
            print(f'\n🏁 Completion: {status} (reason: {reason})')

        else:
            print(f'\n❓ Unknown event type: {event_type}')
            print(f'   Full Event: {json.dumps(event, indent=2)}')

    async def stream_conversation(
        self,
        conversation_id: str,
        api_key: str,
        system_prompt: str = '',
        user_prompt: str = '',
        research_mode: ResearchMode = ResearchMode.DEEP_RESEARCH,
    ):
        """
        Stream conversation responses from the FastAPI endpoint
        """
        params = {
            'conversation_id': conversation_id,
            'system_prompt': system_prompt,
            'user_prompt': user_prompt,
            'research_mode': research_mode.value,
        }

        endpoint = f'{self.base_url}/api/v1/integration/conversations/join-conversation'

        print(f'🔗 Connecting to: {endpoint}')
        print(f'📋 Parameters: {params}')
        print('=' * 50)

        try:
            async with self.client.stream(
                'POST',
                endpoint,
                json=params,
                headers={'Authorization': f'Bearer {api_key}'},
            ) as response:
                print(f'✅ Response Status: {response.status_code}')

                if response.status_code != 200:
                    error_text = await response.aread()
                    print(f'❌ Error: {error_text.decode()}')
                    return

                print('🔄 Streaming events:')
                print('-' * 50)

                # Buffer to handle chunked JSON
                buffer = ''

                async for chunk in response.aiter_text():
                    buffer += chunk

                    # Process complete JSON objects from buffer
                    while buffer:
                        try:
                            # Try to decode JSON from the buffer
                            decoder = json.JSONDecoder()
                            event, idx = decoder.raw_decode(buffer)

                            # Successfully parsed a JSON object
                            await self._handle_event(event)

                            # Remove processed JSON from buffer
                            buffer = buffer[idx:].lstrip()

                            # Check for completion
                            if event.get('type') == 'completion':
                                status = event.get('status', 'finished')
                                if status == 'cancelled':
                                    print(
                                        f"\n🚫 Stream cancelled: {event.get('message', 'Stream was cancelled')}"
                                    )
                                elif status == 'finished':
                                    print(
                                        f"\n✅ Stream completed successfully with message: {event.get('message', 'Unknown message')}"
                                    )
                                else:
                                    print(
                                        f"\n🏁 Stream ended with status '{status}': {event.get('message', 'No message')}"
                                    )
                                return
                            elif event.get('type') == 'error':
                                print(
                                    f"\n❌ Stream ended with error: {event.get('message', 'Unknown error')}"
                                )
                                return

                        except json.JSONDecodeError:
                            # Incomplete JSON in buffer, wait for more data
                            break

                # Handle any remaining buffer content
                if buffer.strip():
                    print(f'⚠️  Unparsed buffer content: {buffer}')

        except httpx.ConnectError:
            print(f'❌ Failed to connect to {self.base_url}')
            print('Make sure your FastAPI server is running!')
        except httpx.TimeoutException:
            print('⏰ Request timed out')
        except Exception as e:
            print(f'❌ Unexpected error: {e}')


async def main():
    """
    Main function to run the streaming client
    """
    # Example configuration - modify these values
    config = {
        'conversation_id': '4b03707134ee42b4abf613353f746b6c',
        'api_key': os.getenv('API_KEY'),
        'system_prompt': 'You are a helpful AI assistant specialized in software development. You are also a joke teller.',
        'user_prompt': 'Tell me a joke.',
        'research_mode': ResearchMode.DEEP_RESEARCH,
    }

    # FastAPI server URL

    print('🚀 Starting FastAPI Streaming Client')

    async with StreamingClient(os.getenv('API_BASE_URL')) as client:
        await client.stream_conversation(**config)


def print_usage():
    """Print usage information"""
    print(
        """
FastAPI Streaming Client

This client connects to your FastAPI streaming endpoint and displays
real-time responses from the socket.io conversation.

Before running:
1. Start your socket.io server (usually on port 3000)
2. Start your FastAPI server with the streaming endpoint (usually on port 8000)
3. Update the configuration in this script with your actual values

Usage:
    python fastapi_streaming_client.py
    """
    )


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] in ['-h', '--help', 'help']:
        print_usage()
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            print('\n⏹️  Client stopped by user')
        except Exception as e:
            print(f'\n💥 Client error: {e}')
