"""Backend capabilities belong to configured instances, not transport endpoints."""

import pytest

from kkrunner.backends import (
    DirectBackend, PsijBackend, SupervisedBackend, backend_class, configured_backend,
)
from kkrunner.backends.contracts import BackendCapability, RecoveryDisposition
from kkrunner.client import LocalRunEndpoint, SshRunEndpoint
from kkrunner.client.contracts import RunEndpoint
from kkrunner.client.endpoint import ExecutionEndpointConfig, describe_endpoint, endpoint_from_config
from kkrunner.client.transport import SshTransport
from kkrunner.backends.support.class_helper import static_methods
from kkrunner.client.operations import BackendOperation, operation_command


MANAGED = frozenset({BackendCapability.REATTACH, BackendCapability.RECOVER})


def test_capabilities_are_immutable_and_instance_specific():
    assert DirectBackend().supports == frozenset()
    assert SupervisedBackend().supports == MANAGED
    assert PsijBackend().supports == MANAGED
    assert PsijBackend(executor_name="SLURM").supports == MANAGED
    for name in ("pbs", "local", "flux"):
        assert PsijBackend(executor_name=name).supports == frozenset({BackendCapability.REATTACH})
    assert PsijBackend(executor_name="slurm", url="ssh://scheduler").supports == frozenset({BackendCapability.REATTACH})


def test_backend_owns_its_options_without_a_second_endpoint_copy():
    backend = PsijBackend(executor_name="pbs")
    for endpoint in (
        LocalRunEndpoint(backend),
        SshRunEndpoint(SshTransport("example.org"), backend=backend),
    ):
        assert endpoint.backend is backend
        assert not hasattr(endpoint, "backend_options")
        assert not hasattr(endpoint, "supports_reattachment")
        assert not hasattr(endpoint, "requires_backend_identity")
        assert endpoint.backend.supports == frozenset({BackendCapability.REATTACH})
        assert dict(endpoint.backend.options) == {"executor_name": "pbs", "url": None}
    assert not hasattr(RunEndpoint, "supports_reattachment")
    with pytest.raises(TypeError):
        backend.options["executor_name"] = "slurm"
    with pytest.raises(ValueError, match="existing backend instance"):
        configured_backend(backend, options={"executor_name": "slurm"})


def test_create_backend_from_name_class_or_instance():
    assert isinstance(configured_backend("direct"), DirectBackend)
    assert isinstance(configured_backend(SupervisedBackend), SupervisedBackend)
    backend = PsijBackend(executor_name="pbs")
    assert configured_backend(backend) is backend
    assert isinstance(LocalRunEndpoint("process").backend, SupervisedBackend)
    assert isinstance(SshRunEndpoint(SshTransport("example"), backend="psij").backend, PsijBackend)
    with pytest.raises(ValueError, match="Unknown"):
        configured_backend(PsijBackend, options={"executor": "pbs"})


@pytest.mark.parametrize("location", ("local", "ssh"))
@pytest.mark.parametrize("executor_name,url,recover", (
    ("slurm", None, True), ("SLURM", None, True), ("pbs", None, False),
    ("slurm", "remote://pbs", False),
))
def test_configuration_roundtrip_preserves_capabilities(location, executor_name, url, recover):
    backend = PsijBackend(executor_name=executor_name, url=url)
    endpoint = (
        LocalRunEndpoint(backend) if location == "local"
        else SshRunEndpoint(SshTransport("example.org"), backend=backend)
    )
    configuration = describe_endpoint(endpoint)
    restored = endpoint_from_config(configuration)
    assert isinstance(restored.backend, PsijBackend)
    assert restored.backend is not backend
    assert dict(restored.backend.options) == dict(backend.options)
    assert describe_endpoint(restored) == configuration
    assert (BackendCapability.RECOVER in restored.backend.supports) is recover
    assert configuration == ExecutionEndpointConfig(**vars(configuration))


@pytest.mark.parametrize("location", ("local", "ssh"))
def test_unsupported_recovery_is_rejected_before_transport_use(location):
    backend = PsijBackend(executor_name="pbs")
    endpoint = (
        LocalRunEndpoint(backend) if location == "local"
        else SshRunEndpoint(SshTransport("invalid.invalid"), backend=backend)
    )
    result = endpoint.recover_submission("irrelevant-key", object())
    assert result.disposition is RecoveryDisposition.UNSUPPORTED


def test_package_layout_and_ssh_operation_are_class_based():
    for kind in ("direct", "process", "psij"):
        backend_type = backend_class(kind)
        assert backend_type.__module__.endswith(".backend")
        assert static_methods(backend_type)
        operation = BackendOperation.for_backend(backend_type, "stream")
        command = operation_command(operation, streaming=True)
        assert "-c" in command
        compile(command[-1], "<SSH backend operation>", "exec")


def test_ssh_dispatch_uses_class_and_request_uses_backend_instance_options():
    backend = PsijBackend(executor_name="pbs")
    local = LocalRunEndpoint(backend)
    ssh = SshRunEndpoint(SshTransport("example.org"), backend=backend)
    assert local._operation("status").backend is PsijBackend
    assert ssh._operation("status").backend is PsijBackend
    request = ssh.submission_request({}, submission_key="id")
    assert request["backend"] == "psij"
    assert request["backend_options"] == {"executor_name": "pbs", "url": None}
    assert request["id"] == "id"


def test_psij_constructor_validates_options():
    with pytest.raises(ValueError, match="nonempty"):
        PsijBackend(executor_name="")
    with pytest.raises(ValueError, match="nonempty"):
        PsijBackend(url="")
