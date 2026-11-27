"""Explicit source dependencies work without an installed target package."""

import subprocess
import sys

import pytest

from kkrunner.client.contracts import Operation
from kkrunner.targets.implementations.ssh.rpc import operation_command
from kkrunner.targets.rpc import operation_payload, operation_result
from kkrunner.targets.implementations.ssh.source import function_definition, portable_bundle
from kkrunner.support.decorators import depends_on, portable


@portable
def increment(value):
    return value + 1


@portable
@depends_on(increment)
def twice_increment(value):
    return increment(increment(value))


@portable
@depends_on(increment, twice_increment)
def dependent_operation(value):
    return increment(twice_increment(value))


@depends_on(increment)
@portable
def reversed_decorators(value):
    return increment(value)


@portable
def undeclared_operation(value):
    return increment(value)


def test_transitive_dependencies_execute_in_isolated_stdlib_python(tmp_path):
    command = operation_command(
        Operation.standalone(dependent_operation), python=(sys.executable, "-I", "-S"),
    )
    assert command[-1].count("def increment(") == 1
    reply = subprocess.run(command, input=operation_payload((39,), {}),
                           text=True, capture_output=True, check=True, cwd=tmp_path)
    assert operation_result(reply.stdout) == 42
    assert not reply.stderr


@pytest.mark.parametrize("function,expected", [
    (increment, 40), (twice_increment, 41), (reversed_decorators, 40),
])
def test_portable_functions_support_both_decorator_orders_and_no_dependencies(function, expected):
    # Both source delivery formats must execute the same declared dependency graph.
    namespace = {}
    exec(function_definition(function), namespace)
    assert namespace[function.__name__](39) == expected

    entrypoint, definitions = portable_bundle(function)
    namespace = {}
    for _name, source in definitions:
        exec(source, namespace)
    assert namespace[entrypoint](39) == expected


def test_source_extraction_does_not_validate_undeclared_globals():
    # The marker is a declaration; direct-backend portability is tested separately.
    definition = function_definition(undeclared_operation)
    assert "def undeclared_operation(" in definition
    assert "def increment(" not in definition


def test_dependency_cycles_are_rejected(monkeypatch):
    monkeypatch.setattr(increment, "__source_dependencies__", (twice_increment,))
    for assemble in (function_definition, portable_bundle):
        with pytest.raises(TypeError, match="Cyclic"):
            assemble(dependent_operation)


def test_conflicting_dependency_names_are_rejected():
    def function(source, **globals):
        namespace = dict(globals, __name__="test_dependencies")
        exec(source, namespace)  # noqa: S102 - controlled test-only function sources
        value = namespace[next(key for key in namespace if key not in globals
                               and not key.startswith("__"))]
        value.__source__ = source
        return portable(value)

    left = function("def helper(value): return value + 1")
    right = function("def helper(value): return value + 2")
    first = depends_on(left)(function("def first(value): return helper(value)", helper=left))
    second = depends_on(right)(function("def second(value): return helper(value)", helper=right))
    combined = depends_on(first, second)(function(
        "def combined(value): return first(value) + second(value)",
        first=first, second=second,
    ))
    with pytest.raises(TypeError, match="Conflicting"):
        function_definition(combined)
