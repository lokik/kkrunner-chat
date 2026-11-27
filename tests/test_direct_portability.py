"""Check direct backend's portable source contract without runtime validation.

Every direct static operation is discovered automatically; dependencies are
checked transitively, including nested functions' global references.
"""

import ast
import dis
import inspect
import textwrap
import types

import pytest

from kkrunner.backends.implementations.direct.backend import DirectBackend
from kkrunner.client.contracts import Operation
from kkrunner.targets.implementations.ssh.rpc import operation_command
from kkrunner.support.decorators import portable


def _global_names(code):
    instructions = tuple(dis.get_instructions(code))
    for index, instruction in enumerate(instructions):
        if instruction.opname == "LOAD_NAME" and instruction.argval == "__name__":
            following = instructions[index + 1] if index + 1 < len(instructions) else None
            if following is not None and following.opname == "STORE_NAME" and following.argval == "__module__":
                continue
        if instruction.opname in ("LOAD_GLOBAL", "LOAD_NAME"):
            yield instruction.argval
    for constant in code.co_consts:
        if isinstance(constant, types.CodeType):
            yield from _global_names(constant)


def _validate_portable_graph(function, *, method=False):
    visited = {}
    visiting = set()

    def visit(fn, *, allow_method=False):
        assert inspect.isfunction(fn), f"Not a function: {fn!r}"
        assert getattr(fn, "__kkrunner_portable__", False), f"Missing @portable: {fn!r}"
        assert allow_method or fn.__qualname__ == fn.__name__, f"Not top-level: {fn!r}"
        assert fn.__closure__ is None, f"Closure in {fn!r}"
        if fn in visiting:
            pytest.fail(f"Cyclic @depends_on graph: {fn!r}")
        if fn in visited:
            return
        visiting.add(fn)
        deps = getattr(fn, "__source_dependencies__", ())
        bindings = {}
        for dep in deps:
            assert inspect.isfunction(dep), f"Non-function dependency of {fn!r}"
            binding = dep.__name__
            assert fn.__globals__.get(binding) is dep, f"Dependency not bound as {binding}"
            assert binding not in bindings, f"Duplicate dependency: {binding}"
            bindings[binding] = dep
            visit(dep)
        visiting.remove(fn)
        visited[fn] = bindings

    visit(function, allow_method=method)
    namespace = {}
    for fn in visited:
        name = fn.__name__
        assert name not in namespace or namespace[name] is fn, f"Conflicting name: {name}"
        namespace[name] = fn

    for fn, declared in visited.items():
        used = set(_global_names(fn.__code__))
        implicit = {
            name for name in used
            if (name in fn.__globals__ or name in namespace) and name not in declared
        }
        assert not implicit, f"Undeclared globals in {fn.__qualname__}: {sorted(implicit)}"
        source = textwrap.dedent(getattr(fn, "__source__", None) or inspect.getsource(fn))
        nodes = ast.parse(source).body
        assert len(nodes) == 1 and isinstance(nodes[0], (ast.FunctionDef, ast.AsyncFunctionDef))
        definition = nodes[0]
        # Generated bindings and remote protocol names are reserved.
        for node in ast.walk(definition):
            identifiers = []
            if isinstance(node, ast.Name):
                identifiers.append(node.id)
            if isinstance(node, ast.arg):
                identifiers.append(node.arg)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                identifiers.append(node.name)
            if isinstance(node, ast.alias):
                identifiers.append(node.asname or node.name.split(".")[0])
            assert not any(value.startswith("_kkrunner_") for value in identifiers), (
                f"Reserved _kkrunner_ identifier in {fn.__qualname__}"
            )
        for decorator in definition.decorator_list:
            if isinstance(decorator, ast.Name):
                assert decorator.id in {"portable", "staticmethod"}, (
                    f"Unexpected decorator in {fn.__qualname__}: {ast.unparse(decorator)}"
                )
            else:
                assert (
                    isinstance(decorator, ast.Call)
                    and isinstance(decorator.func, ast.Name)
                    and decorator.func.id == "depends_on"
                ), f"Unexpected decorator in {fn.__qualname__}: {ast.unparse(decorator)}"
        for default in (
            *definition.args.defaults,
            *(d for d in definition.args.kw_defaults if d is not None),
        ):
            try:
                ast.literal_eval(default)
            except (ValueError, TypeError, SyntaxError):
                pytest.fail(f"Non-literal portable default in {fn.__qualname__}")


_DIRECT_METHODS = tuple(
    name for name, member in vars(DirectBackend).items()
    if isinstance(member, staticmethod)
)


@pytest.mark.parametrize("name", _DIRECT_METHODS)
def test_direct_source_is_portable(name):
    fn = getattr(DirectBackend, name)
    _validate_portable_graph(fn, method=fn.__qualname__ != fn.__name__)
    # Delivered code must compile without local decorations or annotations.
    argv = operation_command(
        Operation.for_backend(DirectBackend, name), framed=name == "stream"
    )
    compile(argv[-1], f"<remote DirectBackend.{name}>", "exec")


def test_portable_is_declarative_only():
    def example():
        return 1

    assert portable(example) is example
    assert example.__kkrunner_portable__ is True
