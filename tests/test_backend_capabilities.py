"""Backend capabilities belong to backend classes, not endpoint protocols."""

from kkrunner.backends import DirectBackend, PsijBackend, SupervisedBackend, backend_class
from kkrunner.backends.contracts import BackendCapability
from kkrunner.client import LocalRunEndpoint, SshRunEndpoint
from kkrunner.client.contracts import RunEndpoint
from kkrunner.client.transport import SshTransport
from kkrunner.backends.support.class_helper import static_methods
from kkrunner.client.operations import BackendOperation, operation_command


def test_backend_capability_declarations():
    assert isinstance(DirectBackend.supports, frozenset)
    assert DirectBackend.supports == frozenset()
    expected = frozenset({BackendCapability.REATTACH, BackendCapability.RECOVER})
    assert SupervisedBackend.supports == expected
    assert PsijBackend.supports == expected


def test_endpoint_does_not_own_backend_capabilities():
    assert not hasattr(RunEndpoint, "supports_reattachment")
    for backend in (DirectBackend, SupervisedBackend, PsijBackend):
        for endpoint in (LocalRunEndpoint(backend), SshRunEndpoint(SshTransport("example.org"), backend=backend)):
            assert endpoint.backend is backend
            assert endpoint.backend.supports is backend.supports
            assert not hasattr(endpoint, "supports_reattachment")
            assert not hasattr(endpoint, "requires_backend_identity")


def test_backend_package_layout_and_ssh_dispatch():
    for kind in ("direct", "process", "psij"):
        backend = backend_class(kind)
        assert backend.__module__.endswith(".backend")
        assert static_methods(backend)
        operation = BackendOperation.for_backend(backend, "stream")
        command = operation_command(operation, streaming=True)
        assert "-c" in command
        compile(command[-1], "<SSH backend operation>", "exec")
