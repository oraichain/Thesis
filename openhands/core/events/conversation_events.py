"""
Conversation Events for distributed processing.

This module defines the event types used in the message queue system
for communication between API servers and workers in multi-worker mode.
"""

import time
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class ConversationEventType(str, Enum):
    """
    Enumeration of conversation event types.
    """

    NEW_CONVERSATION = 'new_conversation'
    PROCESS_CONVERSATION = 'process_conversation'
    CONVERSATION_COMPLETE = 'conversation_complete'
    CONVERSATION_ERROR = 'conversation_error'
    USER_ACTION = 'user_action'


class ConversationEvent(BaseModel):
    """
    Base class for all conversation-related events.

    Attributes:
        conversation_id: Unique identifier for the conversation
        event_type: Type of the event
        timestamp: Event timestamp
        metadata: Additional metadata for the event
    """

    conversation_id: str = Field(
        ..., description='Unique identifier for the conversation'
    )
    event_type: ConversationEventType = Field(..., description='Type of the event')
    timestamp: float = Field(..., description='Event timestamp')
    metadata: Dict[str, Any] = Field(
        default_factory=dict, description='Additional metadata'
    )


class NewConversationEvent(ConversationEvent):
    """
    Event published when a new conversation is created.

    This event is published by the API server and consumed by workers
    to start processing the conversation.
    """

    event_type: Literal[ConversationEventType.NEW_CONVERSATION] = (
        ConversationEventType.NEW_CONVERSATION
    )

    # Conversation initialization data
    conversation_id: str = Field(..., description='Conversation ID')
    conversation_init_data: Dict[str, Any] = Field(
        ..., description='Conversation initialization data'
    )
    user_id: str = Field(..., description='User ID who created the conversation')
    initial_user_msg: Optional[str] = Field(None, description='Initial user message')
    image_urls: List[str] = Field(
        default_factory=list, description='Image URLs attached to initial message'
    )
    replay_json: Optional[str] = Field(
        None, description='Replay JSON for conversation replay'
    )
    system_prompt: Optional[str] = Field(
        None, description='System prompt for the conversation'
    )
    user_prompt: Optional[str] = Field(None, description='User prompt template')
    github_user_id: Optional[str] = Field(None, description='GitHub user ID')
    mnemonic: Optional[str] = Field(None, description='User mnemonic')
    mcp_disable: Optional[Dict[str, bool]] = Field(
        None, description='MCP tools to disable'
    )
    knowledge_base: Optional[List[Dict]] = Field(
        None, description='Knowledge base data'
    )
    space_id: Optional[int] = Field(None, description='Space ID')
    thread_follow_up: Optional[int] = Field(None, description='Thread follow-up ID')
    research_mode: Optional[str] = Field(None, description='Research mode')
    raw_followup_conversation_id: Optional[str] = Field(
        None, description='Raw follow-up conversation ID'
    )
    space_section_id: Optional[int] = Field(None, description='Space section ID')
    output_config: Optional[Dict] = Field(None, description='Output configuration')
    attach_convo_id: bool = Field(
        False, description='Whether to attach conversation ID to message'
    )


class UserActionEvent(BaseModel):
    """
    Event published when a user action is performed.
    """

    event_type: Literal[ConversationEventType.USER_ACTION] = (
        ConversationEventType.USER_ACTION
    )
    event_data: Dict[str, Any] = Field(..., description='Event data from user action')
    event_type_worker: Optional[str] = Field(None, description='Event type from worker')
    conversation_id: str = Field(..., description='Conversation ID')
    user_id: str = Field(..., description='User ID')


class ProcessConversationEvent(ConversationEvent):
    """
    Event published when conversation processing updates are available.

    This event is published by workers and consumed by API servers
    to send updates to clients via the API event stream.
    """

    event_type: Literal[ConversationEventType.PROCESS_CONVERSATION] = (
        ConversationEventType.PROCESS_CONVERSATION
    )

    # Event data from worker processing
    event_data: Dict[str, Any] = Field(
        ..., description='Event data from worker processing'
    )
    event_id: Optional[str] = Field(None, description='Event ID from the worker')
    event_type_worker: Optional[str] = Field(None, description='Event type from worker')
    source: str = Field(..., description='Source of the event (worker name)')
    conversation_id: str = Field(..., description='Conversation ID')
    user_id: str = Field(..., description='User ID')

    # Status information
    status: Optional[str] = Field(
        None, description='Current status of conversation processing'
    )
    progress: Optional[float] = Field(
        None, description='Processing progress (0.0 to 1.0)'
    )
    error_message: Optional[str] = Field(
        None, description='Error message if processing failed'
    )


class ConversationCompleteEvent(ConversationEvent):
    """
    Event published when conversation processing is complete.

    This event indicates that the worker has finished processing
    and the conversation can be considered complete.
    """

    event_type: Literal[ConversationEventType.CONVERSATION_COMPLETE] = (
        ConversationEventType.CONVERSATION_COMPLETE
    )

    # Completion data
    final_result: Optional[str] = Field(
        None, description='Final result of the conversation'
    )
    total_tokens: Optional[int] = Field(None, description='Total tokens used')
    processing_time: Optional[float] = Field(
        None, description='Total processing time in seconds'
    )
    success: bool = Field(True, description='Whether processing was successful')


class ConversationErrorEvent(ConversationEvent):
    """
    Event published when conversation processing encounters an error.

    This event is used to communicate errors from workers to API servers.
    """

    event_type: Literal[ConversationEventType.CONVERSATION_ERROR] = (
        ConversationEventType.CONVERSATION_ERROR
    )

    # Error information
    error_type: str = Field(..., description='Type of error that occurred')
    error_message: str = Field(..., description='Human-readable error message')
    error_details: Optional[Dict[str, Any]] = Field(
        None, description='Additional error details'
    )
    recoverable: bool = Field(False, description='Whether the error is recoverable')


def create_new_conversation_event(
    conversation_id: str,
    conversation_init_data: Dict[str, Any],
    user_id: Optional[str] = None,
    **kwargs,
) -> NewConversationEvent:
    """
    Factory function to create a NewConversationEvent.

    Args:
        conversation_id: Unique identifier for the conversation
        conversation_init_data: Conversation initialization data
        user_id: User ID who created the conversation
        **kwargs: Additional event parameters

    Returns:
        NewConversationEvent: Configured event
    """
    return NewConversationEvent(
        conversation_id=conversation_id,
        timestamp=time.time(),
        conversation_init_data=conversation_init_data,
        user_id=user_id,
        **kwargs,
    )


def create_user_action_event(
    conversation_id: str,
    user_id: str,
    event_data: Dict[str, Any],
    **kwargs,
) -> UserActionEvent:
    """
    Factory function to create a UserActionEvent.
    """
    return UserActionEvent(
        conversation_id=conversation_id,
        user_id=user_id,
        timestamp=time.time(),
        event_data=event_data,
        **kwargs,
    )


def create_process_conversation_event(
    conversation_id: str,
    user_id: str,
    event_data: Dict[str, Any],
    source: str,
    **kwargs,
) -> ProcessConversationEvent:
    """
    Factory function to create a ProcessConversationEvent.

    Args:
        conversation_id: Unique identifier for the conversation
        event_data: Event data from worker processing
        source: Source of the event (worker name)
        **kwargs: Additional event parameters

    Returns:
        ProcessConversationEvent: Configured event
    """
    return ProcessConversationEvent(
        conversation_id=conversation_id,
        user_id=user_id,
        timestamp=time.time(),
        event_data=event_data,
        source=source,
        **kwargs,
    )


def create_conversation_complete_event(
    conversation_id: str, success: bool = True, **kwargs
) -> ConversationCompleteEvent:
    """
    Factory function to create a ConversationCompleteEvent.

    Args:
        conversation_id: Unique identifier for the conversation
        success: Whether processing was successful
        **kwargs: Additional event parameters

    Returns:
        ConversationCompleteEvent: Configured event
    """
    return ConversationCompleteEvent(
        conversation_id=conversation_id,
        timestamp=time.time(),
        success=success,
        **kwargs,
    )


def create_conversation_error_event(
    conversation_id: str, error_type: str, error_message: str, **kwargs
) -> ConversationErrorEvent:
    """
    Factory function to create a ConversationErrorEvent.

    Args:
        conversation_id: Unique identifier for the conversation
        error_type: Type of error that occurred
        error_message: Human-readable error message
        **kwargs: Additional event parameters

    Returns:
        ConversationErrorEvent: Configured event
    """
    return ConversationErrorEvent(
        conversation_id=conversation_id,
        timestamp=time.time(),
        error_type=error_type,
        error_message=error_message,
        **kwargs,
    )
