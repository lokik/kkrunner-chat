"""One registry for backend-side operations, used by local code and the agent."""

from importlib import import_module

from kkrunner.backends.handles import _decode_handle

_BACKENDS = {
    "process": "kkrunner.backends.process.operations",
    "psij": "kkrunner.backends.psij.operations",
}


def backend_for_kind(kind):
    try:
        module = _BACKENDS[kind]
    except KeyError as exc:
        raise ValueError(f"Unsupported backend: {kind!r}") from exc
    return import_module(module)


def backend_for_request(request):
    return backend_for_kind(request.get("executor", "process"))


def backend_for_ref(ref):
    return backend_for_kind(_decode_handle(ref)["kind"])
