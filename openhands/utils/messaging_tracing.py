"""
Utilities for tracing messages through message queues.

This module provides common utilities for propagating trace context through
message queues, enabling end-to-end distributed tracing across publisher/consumer
boundaries in the OpenHands worker system.
"""

from typing import Any, Dict, Optional

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# Global tracer and propagator instances
tracer = trace.get_tracer(__name__)
propagator = TraceContextTextMapPropagator()

# Constants for trace context keys
TRACE_KEY_PREFIX = '_trace_'


def inject_trace_context(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Inject current trace context into a message.

    Args:
        message: The message dictionary to inject trace context into

    Returns:
        The message with trace context injected
    """
    # Create a copy to avoid modifying the original
    enhanced_message = message.copy()

    # Inject trace context
    trace_context: Dict[str, Any] = {}
    propagator.inject(trace_context)

    # Add trace context to message with prefix to avoid conflicts
    for trace_key, trace_value in trace_context.items():
        enhanced_message[f'{TRACE_KEY_PREFIX}{trace_key}'] = trace_value

    return enhanced_message


def extract_trace_context(message_data: Dict[str, Any]) -> Optional[Context]:
    """
    Extract trace context from message data.

    Args:
        message_data: The message data containing trace context

    Returns:
        Extracted trace context or None if no trace context found
    """
    trace_headers = {}

    # Extract trace headers that were injected by the publisher
    for key, value in message_data.items():
        if key.startswith(TRACE_KEY_PREFIX):
            trace_key = key[len(TRACE_KEY_PREFIX) :]  # Remove prefix
            trace_headers[trace_key] = value

    # If trace headers exist, extract the context to continue the trace
    if trace_headers:
        return propagator.extract(trace_headers)
    return None


def start_consumer_span(
    span_name: str,
    message_data: Dict[str, Any],
    messaging_system: str = 'redis',
    destination: Optional[str] = None,
    message_id: Optional[str] = None,
    additional_attributes: Optional[Dict[str, Any]] = None,
):
    """
    Start a consumer span with proper trace context linking.

    Args:
        span_name: Name for the span
        message_data: Message data containing trace context
        messaging_system: The messaging system being used (default: redis)
        destination: The destination/topic/stream name
        message_id: The message ID
        additional_attributes: Additional span attributes

    Returns:
        Context manager for the span
    """
    # Extract trace context from message
    extracted_context = extract_trace_context(message_data)

    # Start span with extracted context as a context manager
    span = tracer.start_as_current_span(
        span_name, context=extracted_context, kind=trace.SpanKind.CONSUMER
    )

    # Set standard messaging attributes on enter
    def set_attributes():
        current_span = trace.get_current_span()
        if current_span:
            current_span.set_attribute('messaging.system', messaging_system)
            current_span.set_attribute('messaging.operation', 'receive')

            if destination:
                current_span.set_attribute('messaging.destination', destination)

            if message_id:
                current_span.set_attribute('messaging.message_id', message_id)

            # Add any additional attributes
            if additional_attributes:
                for key, value in additional_attributes.items():
                    current_span.set_attribute(key, value)

    # Custom context manager that sets attributes on entry
    class SpanContextManager:
        def __enter__(self):
            self._span_cm = span.__enter__()
            set_attributes()
            return self._span_cm

        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type:
                current_span = trace.get_current_span()
                if current_span:
                    current_span.record_exception(exc_val)
                    current_span.set_status(
                        trace.Status(trace.StatusCode.ERROR, str(exc_val))
                    )
            return span.__exit__(exc_type, exc_val, exc_tb)

    return SpanContextManager()


def start_producer_span(
    span_name: str,
    messaging_system: str = 'redis',
    destination: Optional[str] = None,
    message_id: Optional[str] = None,
    additional_attributes: Optional[Dict[str, Any]] = None,
):
    """
    Start a producer span for publishing messages.

    Args:
        span_name: Name for the span
        messaging_system: The messaging system being used (default: redis)
        destination: The destination/topic/stream name
        message_id: The message ID
        additional_attributes: Additional span attributes

    Returns:
        Context manager for the span
    """
    # Start span as producer context manager
    span = tracer.start_as_current_span(span_name, kind=trace.SpanKind.PRODUCER)

    # Set standard messaging attributes on enter
    def set_attributes():
        current_span = trace.get_current_span()
        if current_span:
            current_span.set_attribute('messaging.system', messaging_system)
            current_span.set_attribute('messaging.operation', 'publish')

            if destination:
                current_span.set_attribute('messaging.destination', destination)

            if message_id:
                current_span.set_attribute('messaging.message_id', message_id)

            # Add any additional attributes
            if additional_attributes:
                for key, value in additional_attributes.items():
                    current_span.set_attribute(key, value)

    # Custom context manager that sets attributes on entry
    class SpanContextManager:
        def __enter__(self):
            self._span_cm = span.__enter__()
            set_attributes()
            return self._span_cm

        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type:
                current_span = trace.get_current_span()
                if current_span:
                    current_span.record_exception(exc_val)
                    current_span.set_status(
                        trace.Status(trace.StatusCode.ERROR, str(exc_val))
                    )
            return span.__exit__(exc_type, exc_val, exc_tb)

    return SpanContextManager()


def clean_trace_context_from_message(message_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove trace context keys from message data for cleaner processing.

    Args:
        message_data: The message data to clean

    Returns:
        Cleaned message data without trace context keys
    """
    cleaned_data = {}

    for key, value in message_data.items():
        if not key.startswith(TRACE_KEY_PREFIX):
            cleaned_data[key] = value

    return cleaned_data


def get_trace_id() -> Optional[str]:
    """
    Get the current trace ID as a string.

    Returns:
        Current trace ID or None if no active trace
    """
    current_span = trace.get_current_span()
    if current_span and current_span.is_recording():
        trace_id = current_span.get_span_context().trace_id
        # Convert to hex string with leading zeros
        return f'{trace_id:032x}'
    return None


def get_span_id() -> Optional[str]:
    """
    Get the current span ID as a string.

    Returns:
        Current span ID or None if no active span
    """
    current_span = trace.get_current_span()
    if current_span and current_span.is_recording():
        span_id = current_span.get_span_context().span_id
        # Convert to hex string with leading zeros
        return f'{span_id:016x}'
    return None


def log_trace_info(logger, message: str, **kwargs):
    """
    Log a message with trace context information.

    Args:
        logger: Logger instance to use
        message: Message to log
        **kwargs: Additional context to include in log
    """
    trace_id = get_trace_id()
    span_id = get_span_id()

    extra_context = {}
    if trace_id:
        extra_context['trace_id'] = trace_id
    if span_id:
        extra_context['span_id'] = span_id

    # Add any additional context
    extra_context.update(kwargs)

    logger.info(message, extra=extra_context)
