"""Backend capabilities belong to configured instances, not transport endpoints."""

import pytest

from kkrunner.backends import (
    DirectBackend, PsijBackend, SupervisedBackend, backend_class, configured_backend,
)
from kkrunner.backends.contracts import BackendCapability, RecoveryDisposition
from kkrunner.client import EndpointClient, LocalTransport, SshTransport
from kkrunner.backends.config import BackendConfig
from kkrunner.backends.support.class_helper import static_methods
from kkrunner.client.contracts import Operation
from kkrunner.client.transports.rpc import operation_command


MANAGED = frozenset({BackendCapability.REATTACH, BackendCapability.RECOVER, BackendCapability.STATUS, BackendCapability.CANCEL})


def test_capabilities_are_immutable_and_instance_specific():
    assert DirectBackend().supports == frozenset({BackendCapability.CANCEL})
    assert SupervisedBackend().supports == MANAGED
    assert PsijBackend().supports == MANAGED
    assert PsijBackend(executor_name="SLURM").supports == MANAGED
    for name in ("pbs", "local", "flux"):
        assert PsijBackend(executor_name=name).supports == frozenset({BackendCapability.REATTACH, BackendCapability.STATUS, BackendCapability.CANCEL})
    assert PsijBackend(executor_name="slurm", url="ssh://scheduler").supports == frozenset({BackendCapability.REATTACH, BackendCapability.STATUS, BackendCapability.CANCEL})


def test_backend_owns_its_options_without_a_second_endpoint_copy():
    backend = PsijBackend(executor_name="pbs")
    for endpoint in (
        EndpointClient(backend),
        EndpointClient(backend, transport=SshTransport("example.org")),
    ):
        assert endpoint.backend is backend
        assert not hasattr(endpoint, "backend_options")
        assert not hasattr(endpoint, "supports_reattachment")
        assert not hasattr(endpoint, "requires_backend_identity")
        assert endpoint.backend.supports == frozenset({BackendCapability.REATTACH, BackendCapability.STATUS, BackendCapability.CANCEL})
        assert dict(endpoint.backend.options) == {"executor_name": "pbs", "url": None}
    assert not hasattr(EndpointClient, "supports_reattachment")
    with pytest.raises(TypeError):
        backend.options["executor_name"] = "slurm"
    with pytest.raises(ValueError, match="existing backend instance"):
        configured_backend(backend, options={"executor_name": "slurm"})


def test_create_backend_from_name_class_or_instance():
    assert isinstance(configured_backend("direct"), DirectBackend)
    assert isinstance(configured_backend(SupervisedBackend), SupervisedBackend)
    backend = PsijBackend(executor_name="pbs")
    assert configured_backend(backend) is backend
    assert isinstance(EndpointClient("process").backend, SupervisedBackend)
    assert isinstance(EndpointClient("psij", transport=SshTransport("example")).backend, PsijBackend)
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
        EndpointClient(backend) if location == "local"
        else EndpointClient(backend, transport=SshTransport("example.org"))
    )
    transport_config = endpoint.transport.to_dict()
    backend_config = endpoint.backend_config
    restored = EndpointClient.from_config(transport_config, backend_config)
    assert isinstance(restored.backend, PsijBackend)
    assert restored.backend is not backend
    assert dict(restored.backend.options) == dict(backend.options)
    assert restored.transport.to_dict() == transport_config
    assert restored.backend_config == backend_config
    assert (BackendCapability.RECOVER in restored.backend.supports) is recover
    assert backend_config == BackendConfig(backend_config.name, backend_config.options)


@pytest.mark.parametrize("location", ("local", "ssh"))
def test_unsupported_recovery_is_rejected_before_transport_use(location):
    backend = PsijBackend(executor_name="pbs")
    endpoint = (
        EndpointClient(backend) if location == "local"
        else EndpointClient(backend, transport=SshTransport("invalid.invalid"))
    )
    result = endpoint.recover_submission("irrelevant-key", object())
    assert result.disposition is RecoveryDisposition.UNSUPPORTED


def test_package_layout_and_ssh_operation_are_class_based():
    for kind in ("direct", "process", "psij"):
        backend_type = backend_class(kind)
        assert backend_type.__module__.endswith(".backend")
        assert static_methods(backend_type)
        operation = Operation.for_backend(backend_type, "stream")
        command = operation_command(operation, streaming=True)
        assert "-c" in command
        compile(command[-1], "<SSH backend operation>", "exec")


def test_ssh_dispatch_uses_class_and_request_uses_backend_instance_options():
    backend = PsijBackend(executor_name="pbs")
    local = EndpointClient(backend)
    ssh = EndpointClient(backend, transport=SshTransport("example.org"))
    assert Operation.for_backend(type(local.backend), "status").backend is PsijBackend
    assert Operation.for_backend(type(ssh.backend), "status").backend is PsijBackend
    request = ssh.submission_request({}, submission_key="id")
    assert request["backend"] == "psij"
    assert request["backend_options"] == {"executor_name": "pbs", "url": None}
    assert request["submission_key"] == "id"


def test_psij_constructor_validates_options():
    with pytest.raises(ValueError, match="nonempty"):
        PsijBackend(executor_name="")
    with pytest.raises(ValueError, match="nonempty"):
        PsijBackend(url="")
