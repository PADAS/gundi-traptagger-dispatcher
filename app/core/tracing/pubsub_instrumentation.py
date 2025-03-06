import json

from opentelemetry import propagate, context


def load_context_from_attributes(attributes):
    print(f"[tracing.load_context_from_attributes]> attributes: {attributes}")
    carrier = json.loads(attributes.get("tracing_context", "{}"))
    ctx = propagate.extract(carrier=carrier)
    print(f"[tracing.load_context_from_attributes]> ctx: {ctx}")
    context.attach(ctx)


def build_context_headers():
    headers = []
    propagate.inject(headers)
    return headers
