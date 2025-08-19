import json
import os
from functools import wraps
from typing import Optional

from opentelemetry import context as context_api
from opentelemetry import trace
from opentelemetry.semconv_ai import SpanAttributes, TraceloopSpanKindValues
from traceloop.sdk.telemetry import Telemetry
from traceloop.sdk.tracing import get_tracer, set_workflow_name
from traceloop.sdk.tracing.tracing import (
    TracerWrapper,
    get_chained_entity_path,
    set_entity_path,
)
from traceloop.sdk.utils.json_encoder import JSONEncoder


def _should_send_prompts() -> bool:
    """
    Check if content tracing is enabled via environment variable or context override.
    Updated for 0.57b to use new environment variable naming.
    """
    return os.getenv(
        'TRACELOOP_TRACE_CONTENT', 'true'
    ).lower() == 'true' or context_api.get_value('override_enable_content_tracing')


def streaming_llm_workflow(
    name: Optional[str] = None,
    version: Optional[int] = None,
    tlp_span_kind: Optional[TraceloopSpanKindValues] = TraceloopSpanKindValues.WORKFLOW,
):
    """
    Decorator for streaming LLM workflows, updated for 0.57b.
    """
    return streaming_entity_method(
        name=name, version=version, tlp_span_kind=tlp_span_kind
    )


def streaming_entity_method(
    name: Optional[str] = None,
    version: Optional[int] = None,
    tlp_span_kind: Optional[TraceloopSpanKindValues] = TraceloopSpanKindValues.TASK,
):
    """
    Generic decorator for tracing entities (tasks, tools, workflows, agents) in 0.57b.
    """

    def decorate(fn):
        @wraps(fn)
        def wrap(*args, **kwargs):
            # Check if tracing is initialized
            if not TracerWrapper.verify_initialized():
                return fn(*args, **kwargs)

            # Set entity name and span name
            entity_name = name or fn.__name__
            if tlp_span_kind in [
                TraceloopSpanKindValues.WORKFLOW,
                TraceloopSpanKindValues.AGENT,
            ]:
                set_workflow_name(entity_name)
            span_name = (
                f'{entity_name}.{tlp_span_kind.value}' if tlp_span_kind else entity_name
            )

            # Get tracer and start span
            tracer = get_tracer()
            with tracer.start_as_current_span(span_name) as span:
                # Attach context
                ctx = trace.set_span_in_context(span)
                token = context_api.attach(ctx)

                try:
                    # Set chained entity path for tasks and tools
                    if tlp_span_kind in [
                        TraceloopSpanKindValues.TASK,
                        TraceloopSpanKindValues.TOOL,
                    ]:
                        entity_path = get_chained_entity_path(entity_name)
                        set_entity_path(entity_path)
                    else:
                        entity_path = entity_name

                    # Set span attributes
                    if tlp_span_kind:
                        span.set_attribute(
                            SpanAttributes.TRACELOOP_SPAN_KIND, tlp_span_kind.value
                        )
                    span.set_attribute(
                        SpanAttributes.TRACELOOP_ENTITY_NAME, entity_path
                    )
                    if version is not None:
                        span.set_attribute(
                            SpanAttributes.TRACELOOP_ENTITY_VERSION, version
                        )

                    # Capture input if content tracing is enabled
                    if _should_send_prompts():
                        try:
                            span.set_attribute(
                                SpanAttributes.TRACELOOP_ENTITY_INPUT,
                                json.dumps(
                                    {'args': args, 'kwargs': kwargs}, cls=JSONEncoder
                                ),
                            )
                        except TypeError as e:
                            Telemetry().capture_exception(e)

                    # Execute the function
                    result = fn(*args, **kwargs)

                    # Capture output if content tracing is enabled
                    if _should_send_prompts():
                        try:
                            span.set_attribute(
                                SpanAttributes.TRACELOOP_ENTITY_OUTPUT,
                                json.dumps(result, cls=JSONEncoder),
                            )
                        except TypeError as e:
                            Telemetry().capture_exception(e)

                    return result

                finally:
                    # Detach context to clean up
                    context_api.detach(token)

        return wrap

    return decorate
