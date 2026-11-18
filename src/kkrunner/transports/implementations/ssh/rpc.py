"""Assemble one-shot SSH Python operation scripts."""

from __future__ import annotations
from kkrunner.transports.contracts import Operation
from .bootstrap import _json_call, _stream_call, _framed_call
from .helpers import ssh_helper

def operation_command(operation: Operation, *, python=("python3",), streaming=False, framed=False):
    """Compile the selected source and its invocation protocol into Python argv."""
    import ast

    from .source import function_definition

    if operation.function is not None:
        definition = function_definition(operation.function)
        entrypoint = operation.function.__name__
    else:
        definition = function_definition(getattr(ssh_helper(operation.backend), operation.method), static=True)
        entrypoint = operation.method
    bootstrap = _framed_call if framed else _stream_call if streaming else _json_call
    names = {node.name for node in ast.parse(definition).body}

    def unused_name(name):
        while name in names:
            name += "_"
        names.add(name)
        return name

    bootstrap_name = unused_name("_kkrunner_invoke")
    options = ""
    if framed:
        from kkrunner.remote.stream import stream_protocol
        protocol_name = unused_name("_kkrunner_protocol")
        definition += function_definition(stream_protocol, name=protocol_name)
        options = f", {protocol_name}"
    script = (
        definition + function_definition(bootstrap, name=bootstrap_name)
        + f"{bootstrap_name}({entrypoint}{options})\n"
    )
    return (*python, "-u", "-c", script) if streaming or framed else (*python, "-c", script)


