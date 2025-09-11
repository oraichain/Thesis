import json

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from openhands.core.logger import openhands_logger as logger
from openhands.core.schema.action import ActionType
from openhands.core.schema.research import ResearchMode
from openhands.events.action.agent import RecallAction
from openhands.events.action.empty import NullAction
from openhands.events.async_event_store_wrapper import AsyncEventStoreWrapper
from openhands.events.event_store import EventStore
from openhands.events.observation.agent import AgentStateChangedObservation
from openhands.events.observation.empty import NullObservation
from openhands.events.serialization.event import event_to_dict
from openhands.server.auth import (
    get_github_user_id,
    get_user_id,
)
from openhands.server.data_models.conversation_info import ConversationDetailInfo
from openhands.server.modules.conversation import conversation_module
from openhands.server.modules.space import SpaceModule
from openhands.server.routes.integration.join_conversation_client import (
    SocketStreamClient,
)
from openhands.server.routes.manage_conversations import (
    InitSessionRequest,
    get_default_conversation_title,
    new_conversation,
)
from openhands.server.shared import (
    ConversationStoreImpl,
    config,
    conversation_manager,
)
from openhands.storage.data_models.conversation_status import ConversationStatus


# Response Models
class ConversationCreateResponse(BaseModel):
    status: str = Field(
        description="Response status, always 'ok' for successful creation", example='ok'
    )
    conversation_id: str = Field(
        description='Unique identifier for the created conversation',
        example='conv_abc123def456',
    )


class ConversationErrorResponse(BaseModel):
    status: str = Field(
        description="Error status, always 'error' for failures", example='error'
    )
    message: str = Field(
        description='Human-readable error message', example='Settings not found'
    )
    msg_id: str = Field(
        description='Machine-readable error code for categorization',
        example='CONFIGURATION$SETTINGS_NOT_FOUND',
    )


class FastAPIErrorResponse(BaseModel):
    detail: str = Field(
        description='Error details from FastAPI', example='Unauthorized'
    )


class ConversationEvent(BaseModel):
    action: str = Field(description='Type of action/event', example='message')
    source: str = Field(description='Source of the event (user/agent)', example='user')
    message: str = Field(
        description='Content of the message or action',
        example='Please review this code',
    )
    timestamp: str = Field(
        description='ISO timestamp when the event occurred',
        example='2024-01-15T10:30:00Z',
    )


class ConversationDetailResponse(BaseModel):
    conversation_id: str = Field(
        description='Unique conversation identifier', example='conv_abc123def456'
    )
    title: str = Field(description='Conversation title', example='Code Review Session')
    status: str = Field(description='Current conversation status', example='RUNNING')
    created_at: str = Field(
        description='ISO timestamp when conversation was created',
        example='2024-01-15T10:30:00Z',
    )
    last_updated_at: str = Field(
        description='ISO timestamp of last activity', example='2024-01-15T11:45:00Z'
    )
    selected_repository: str | None = Field(
        description='Associated repository if any', example='user/project-repo'
    )
    research_mode: str | None = Field(
        description='Research mode used in conversation', example='deep_research'
    )
    events: list[dict] | None = Field(
        description='List of conversation events/messages', default=None
    )
    final_result: str | dict | None = Field(
        description='Final result if conversation is completed', default=None
    )


conversation_router = APIRouter(
    prefix='/conversations',
    tags=['conversations'],
    responses={
        401: {'description': 'Authentication required'},
        404: {'description': 'Resource not found'},
        500: {'description': 'Internal server error'},
    },
)
chat_router = APIRouter(
    prefix='/chat_researchs',
    tags=['conversations'],
    responses={
        401: {'description': 'Authentication required'},
        500: {'description': 'Internal server error'},
    },
)
deep_research_router = APIRouter(
    prefix='/deep_researchs',
    tags=['conversations'],
    responses={
        401: {'description': 'Authentication required'},
        500: {'description': 'Internal server error'},
    },
)


class CreateNewConversationIntegrationRequest(BaseModel):
    initial_user_msg: str | None = Field(
        None,
        description='Initial message to start the conversation',
        example="What's the new DeFi meta recently that I can ape in?",
    )
    research_mode: ResearchMode | None = Field(
        None, description='Research mode for the conversation', example='deep_research'
    )
    space_id: int | None = Field(
        None,
        description='Your space ID. You can find it via your created space',
        example=123,
    )
    space_section_id: int | None = Field(
        None,
        description='Your space section ID. You can find it via your created space',
        example=456,
    )
    thread_follow_up: int | None = Field(
        None, description='Thread ID for follow-up conversations', example=789
    )
    followup_discover_id: str | None = Field(
        None,
        description='Discovery ID for follow-up research',
        example='discover_abc123',
    )
    mcp_disable: dict[str, bool] | None = Field(
        None,
        description='MCP tools to disable for this conversation',
    )
    system_prompt: str | None = Field(
        None,
        description='Custom system prompt to guide the AI behavior',
        example="You are a DeFi gigachad who's always ahead of the new DeFi meta.",
    )


class CreateChatConversationIntegrationRequest(BaseModel):
    initial_user_msg: str | None = Field(
        None,
        description='Initial message for the chat conversation',
        example="Let's have a casual conversation about DeFi",
    )
    system_prompt: str | None = Field(
        None,
        description="System prompt to set the AI's behavior in chat mode",
        example='You are a friendly AI assistant who explains complex topics simply',
    )


class CreateDeepResearchConversationIntegrationRequest(BaseModel):
    initial_user_msg: str | None = Field(
        None,
        description='Initial research query to begin deep analysis',
        example='Research the latest developments in DeFi',
    )
    mcp_disable: dict[str, bool] | None = Field(
        None,
        description='MCP tools to disable during deep research',
    )
    system_prompt: str | None = Field(
        None,
        description='System prompt for deep research mode behavior',
        example='You are a thorough DeFi researcher who provides comprehensive analysis with citations',
    )


@conversation_router.post(
    '',
    summary='Create New Conversation',
    description='Creates a new conversation with customizable research mode, system prompt, and space association. Supports follow-up threads and MCP tool configuration.',
    response_description='Returns the newly created conversation details including conversation ID',
    response_model=ConversationCreateResponse,
    responses={
        200: {
            'description': 'Conversation created successfully',
            'model': ConversationCreateResponse,
        },
        400: {
            'description': 'Invalid request data or missing required fields',
            'model': ConversationErrorResponse,
        },
        401: {
            'description': 'Authentication token missing or invalid',
            'model': ConversationErrorResponse,
        },
        500: {'description': 'Internal server error', 'model': FastAPIErrorResponse},
    },
)
async def integration_new_conversation(
    request: Request, data: CreateNewConversationIntegrationRequest
):
    new_conversation_data = InitSessionRequest(**data.model_dump())
    new_conversation_result = await new_conversation(request, new_conversation_data)

    try:
        new_conversation_json = json.loads(new_conversation_result.body)
        conversation_id = new_conversation_json.get('conversation_id')
    except Exception:
        conversation_id = None

    if conversation_id and data.space_id and data.space_section_id:
        space_module = SpaceModule(request.headers.get('Authorization'))
        await space_module.update_space_section_history(
            space_id=str(data.space_id),
            section_id=str(data.space_section_id),
            conversation_id=conversation_id,
        )
    return new_conversation_result


@chat_router.post(
    '',
    summary='Thesis.io chat mode with Multi Web Search tool enabled',
    description='Creates a new conversation optimized for Fast Multi Web Search.',
    response_description='Returns chat conversation details with conversation ID',
    response_model=ConversationCreateResponse,
    responses={
        200: {
            'description': 'Chat conversation created successfully',
            'model': ConversationCreateResponse,
        },
        400: {
            'description': 'Invalid chat request data',
            'model': ConversationErrorResponse,
        },
        401: {
            'description': 'Authentication required',
            'model': ConversationErrorResponse,
        },
        500: {'description': 'Internal server error', 'model': FastAPIErrorResponse},
    },
)
async def integration_new_chat_conversation(
    request: Request, data: CreateChatConversationIntegrationRequest
):
    new_conversation_data = InitSessionRequest(
        **data.model_dump(),
        research_mode=ResearchMode.CHAT.value,
    )
    return await new_conversation(request, new_conversation_data)


@deep_research_router.post(
    '',
    summary='Thesis.io deep research mode with curated data and customized DeFi tools.',
    description='Creates a conversation specifically for DeFi research tasks.',
    response_description='Returns deep research conversation with enhanced capabilities',
    response_model=ConversationCreateResponse,
    responses={
        200: {
            'description': 'Deep research conversation created successfully',
            'model': ConversationCreateResponse,
        },
        400: {
            'description': 'Invalid deep research request parameters',
            'model': ConversationErrorResponse,
        },
        401: {
            'description': 'Authentication required for research access',
            'model': ConversationErrorResponse,
        },
        500: {'description': 'Internal server error', 'model': FastAPIErrorResponse},
    },
)
async def integration_new_deep_research_conversation(
    request: Request, data: CreateDeepResearchConversationIntegrationRequest
):
    new_conversation_data = InitSessionRequest(
        **data.model_dump(),
        research_mode=ResearchMode.DEEP_RESEARCH.value,
    )
    return await new_conversation(request, new_conversation_data)


@conversation_router.get(
    '/{conversation_id}',
    summary='Get Conversation Details',
    description='Retrieves comprehensive details for a specific conversation including metadata, event history, and current status. Returns full conversation context with all messages and interactions.',
    response_description='Complete conversation information with events and metadata',
    response_model=ConversationDetailResponse,
    responses={
        200: {
            'description': 'Conversation details retrieved successfully',
            'model': ConversationDetailResponse,
        },
        401: {
            'description': 'Authentication required to access conversation',
            'model': FastAPIErrorResponse,
        },
        404: {
            'description': 'Conversation not found or access denied',
            'model': FastAPIErrorResponse,
        },
        500: {'description': 'Internal server error', 'model': FastAPIErrorResponse},
    },
)
async def integration_get_conversation(
    conversation_id: str, request: Request
) -> ConversationDetailResponse:
    user_id = get_user_id(request)
    conversation = await conversation_module._get_conversation_by_id(conversation_id)
    if not conversation:
        raise HTTPException(status_code=404, detail='Conversation not found')

    default_title = get_default_conversation_title(conversation_id)
    conversation_info = ConversationDetailInfo(
        conversation_id=conversation_id,
        title=default_title,
    )
    conversation_store = await ConversationStoreImpl.get_instance(
        config, user_id, get_github_user_id(request)
    )
    try:
        metadata = await conversation_store.get_metadata(conversation_id)
        is_running = await conversation_manager.is_agent_loop_running(conversation_id)
        title = metadata.title
        if not title:
            title = default_title
        conversation_info = ConversationDetailInfo(
            conversation_id=metadata.conversation_id,
            title=title,
            last_updated_at=metadata.last_updated_at,
            created_at=metadata.created_at,
            selected_repository=metadata.selected_repository,
            status=(
                ConversationStatus.RUNNING
                if is_running
                else ConversationStatus.FINISHED
            ),
        )
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail='Conversation not found')

    # eventstore
    event_store = EventStore(
        conversation_id,
        conversation_manager.file_store,
        conversation.user_id,
    )
    if not event_store:
        # Convert ConversationDetailInfo dataclass to ConversationDetailResponse
        return ConversationDetailResponse(
            conversation_id=conversation_info.conversation_id,
            title=conversation_info.title,
            status=conversation_info.status.value,
            created_at=conversation_info.created_at.isoformat()
            if conversation_info.created_at
            else None,
            last_updated_at=conversation_info.last_updated_at.isoformat()
            if conversation_info.last_updated_at
            else None,
            selected_repository=conversation_info.selected_repository,
            research_mode=conversation_info.research_mode,
            events=conversation_info.events,  # Test expects None when no event store
            final_result=conversation_info.final_result,
        )
    async_store = AsyncEventStoreWrapper(event_store, 0)
    result = []
    streaming_events = []
    async for event in async_store:
        try:
            if not event:
                continue
            if isinstance(
                event,
                (
                    NullAction,
                    NullObservation,
                    RecallAction,
                    AgentStateChangedObservation,
                ),
            ):
                continue
            event_dict = event_to_dict(event)
            if not event_dict:
                continue
            if event_dict.get('source') not in ['user', 'agent']:
                continue
            if event_dict.get('action') == ActionType.STREAMING_MESSAGE:
                streaming_events.append(event_dict)
                continue
            if streaming_events:
                result.append(_handle_streaming_message(streaming_events))
                streaming_events = []
            result.append(event_dict)
        except Exception as e:
            logger.error(f'Error converting event to dict: {str(e)}')
    if streaming_events:
        result.append(_handle_streaming_message(streaming_events))
    conversation_info.events = result
    if getattr(conversation, 'final_result', None):
        conversation_info.final_result = conversation.final_result

    # Convert ConversationDetailInfo dataclass to ConversationDetailResponse
    return ConversationDetailResponse(
        conversation_id=conversation_info.conversation_id,
        title=conversation_info.title,
        status=conversation_info.status.value,
        created_at=conversation_info.created_at.isoformat()
        if conversation_info.created_at
        else None,
        last_updated_at=conversation_info.last_updated_at.isoformat()
        if conversation_info.last_updated_at
        else None,
        selected_repository=conversation_info.selected_repository,
        research_mode=conversation_info.research_mode,
        events=conversation_info.events,
        final_result=conversation_info.final_result,
    )


def _handle_streaming_message(streaming_events: list[dict] | None) -> dict | None:
    if not streaming_events:
        return None
    last_event = streaming_events[-1]
    last_event['message'] = ''.join([e['message'] for e in streaming_events]).strip()
    streaming_events = []
    return last_event


class JoinConversationIntegrationRequest(BaseModel):
    conversation_id: str | None = Field(
        None,
        description='ID of the existing conversation to join',
        example='conv_abc123def456',
    )
    system_prompt: str | None = Field(
        None,
        description='System prompt to apply when joining the conversation',
        example='Continue as an expert software architect',
    )
    user_prompt: str | None = Field(
        None,
        description='Message to send when joining the conversation',
        example='Please review the code we discussed earlier',
    )
    research_mode: str | None = Field(
        None,
        description='Research mode to use in the conversation',
        example='deep_research',
    )


@conversation_router.post(
    '/join-conversation',
    summary='Join Existing Conversation',
    description='Join an existing conversation using conversation ID and API key authentication. Allows real-time participation in ongoing conversations with streaming responses. The user prompt becomes the next message sent to the AI.',
    response_description='Streaming response with real-time conversation updates',
    responses={
        200: {
            'description': 'Successfully joined conversation with streaming response',
            'content': {
                'application/json': {
                    'schema': {
                        'type': 'string',
                        'description': 'Server-Sent Events stream with real-time conversation updates. Events are serialized using event_to_dict() and sent as SSE format.',
                        'examples': [
                            {
                                'id': 1,
                                'timestamp': '2024-01-15T10:45:00.123Z',
                                'source': 'user',
                                'message': 'Please review this code',
                                'action': 'message',
                                'args': {
                                    'content': 'Please review this code',
                                    'image_urls': None,
                                    'wait_for_response': False,
                                },
                            },
                            {
                                'id': 2,
                                'timestamp': '2024-01-15T10:45:30.456Z',
                                'source': 'agent',
                                'message': "I'll analyze the code for you...",
                                'action': 'message',
                                'args': {
                                    'content': "I'll analyze the code for you. Let me start by examining the structure...",
                                    'wait_for_response': False,
                                },
                            },
                            {
                                'id': 3,
                                'timestamp': '2024-01-15T10:45:35.789Z',
                                'source': 'agent',
                                'observation': 'agent_state_changed',
                                'content': '',
                                'extras': {
                                    'agent_state': 'RUNNING',
                                    'reason': 'Starting code analysis',
                                },
                                'success': True,
                            },
                            {
                                'id': 4,
                                'timestamp': '2024-01-15T10:45:45.012Z',
                                'source': 'agent',
                                'action': 'streaming_message',
                                'args': {
                                    'message': 'Looking at the function definitions, I can see several potential issues...',
                                    'finished': False,
                                },
                            },
                        ],
                    }
                }
            },
        },
        400: {
            'description': 'Missing required fields (conversation_id, system_prompt, research_mode, or user_prompt)',
            'model': FastAPIErrorResponse,
        },
        401: {
            'description': 'Invalid or missing Bearer token in Authorization header',
            'model': FastAPIErrorResponse,
        },
        404: {'description': 'Conversation not found', 'model': FastAPIErrorResponse},
        500: {
            'description': 'Failed to establish streaming connection',
            'model': FastAPIErrorResponse,
        },
    },
)
async def join_conversation(request: Request, data: JoinConversationIntegrationRequest):
    if (
        not data.conversation_id
        or not data.system_prompt
        or not data.research_mode
        or not data.user_prompt
    ):
        raise HTTPException(status_code=400, detail='Missing required fields')

    authorization = request.headers.get('Authorization')
    # Parse Bearer token from Authorization header
    if not authorization or not authorization.startswith('Bearer '):
        raise HTTPException(
            status_code=401, detail='Authorization header with Bearer token is required'
        )

    jwt_token = authorization[7:]  # Remove "Bearer " prefix
    if not jwt_token.strip():
        raise HTTPException(status_code=401, detail='API key cannot be empty')

    try:
        client = SocketStreamClient()
        await client.connect(
            conversation_id=data.conversation_id,
            jwt_token=jwt_token,
            api_base_url='http://localhost:3000',  # TODO: make the port configurable
            system_prompt=data.system_prompt,
            research_mode=ResearchMode(data.research_mode),
        )

        async def stream_with_cancellation(
            user_prompt: str, research_mode: ResearchMode
        ):
            """Stream wrapper that monitors client disconnection"""
            async for chunk in client.stream(
                user_prompt=user_prompt,
                research_mode=research_mode,
            ):
                # Check if client disconnected
                if await request.is_disconnected():
                    # Signal cancellation to client and cleanup
                    client.cancel()
                    break
                yield chunk

        return StreamingResponse(
            stream_with_cancellation(
                data.user_prompt, ResearchMode(data.research_mode)
            ),
            media_type='application/json',
            headers={
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Content-Type': 'text/event-stream',
            },
        )
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f'Failed to start streaming: {str(e)}'
        )
