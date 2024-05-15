from opentelemetry.instrumentation.redis.utils import dont_throw
from opentelemetry.instrumentation.utils import (
    _SUPPRESS_INSTRUMENTATION_KEY,
)
#from opentelemetry.trace.status import Status, StatusCode
from opentelemetry import context as context_api


def _set_span_attribute(span, name, value):
    if value is not None:
        if value != "":
            span.set_attribute(name, value)
    return

@dont_throw
def _set_generic_span_attributes(span):
    _set_span_attribute(span, "redis.generic", 1)
    
@dont_throw
def _set_search_attributes(span, kwargs):
    #print("kwargs in _set_search_attributes:", kwargs)
    #print("kwargs.keys in _set_search_attributes:", kwargs.keys())
    _set_span_attribute(
        span,
        "redis.commands.search.query",
        kwargs.get("query"),
    )

@dont_throw
def _set_create_index_attributes(span, kwargs):
    #print("kwargs in _set_create_index_attributes:", kwargs)
    #print("kwargs.keys in _set_create_index_attributes:", kwargs.keys())
    _set_span_attribute(
        span,
        "create_index",
        kwargs.get("fields"),
    )
    _set_span_attribute(
        span,
        "create_index",
        kwargs.get("definition"),
    )
    
# @dont_throw
# def _add_query_result_events(span, kwargs):


def _with_tracer_wrapper(func):
    """Helper for providing tracer for wrapper functions."""

    def _with_tracer(tracer, to_wrap):
        def wrapper(wrapped, instance, args, kwargs):
            return func(tracer, to_wrap, wrapped, instance, args, kwargs)

        return wrapper

    return _with_tracer


@_with_tracer_wrapper
def _wrap(tracer, to_wrap, wrapped, instance, args, kwargs):
    """Instruments and calls every function defined in TO_WRAP."""
    
    print("kwargs in _wrap:", kwargs)
    print("kwargs.keys in _wrap:", kwargs.keys())
    
    if context_api.get_value(_SUPPRESS_INSTRUMENTATION_KEY):
        return wrapped(*args, **kwargs)

    name = to_wrap.get("span_name")
    print("span name: ", name)
    with tracer.start_as_current_span(name) as span:  
        #span.set_attribute(SpanAttributes.DB_SYSTEM, "redis")
        #span.set_attribute(SpanAttributes.DB_OPERATION, to_wrap.get("method"))
        print("method: ", to_wrap.get("method"))
        if to_wrap.get("method") == "search":
            #_set_search_attributes(span, kwargs)
            _set_generic_span_attributes(span)
        elif to_wrap.get("method") == "create_index":
            #_set_create_index_attributes(span, kwargs)
            _set_generic_span_attributes(span)
        else:
            _set_generic_span_attributes(span)
            
        response = wrapped(*args, **kwargs)
        
        print("response", response)
        # if to_wrap.get("method") == "search":
        #     _add_search_result_events(span, response)
        
        return response