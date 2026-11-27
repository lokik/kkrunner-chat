"""Backend capabilities belong to configured instances, not transport endpoints."""

import pytest

from kkrunner.backends import (
    DirectBackend, PsijBackend, SupervisedBackend, backend_class, configured_backend,
)
from kkrunner.backends.contracts import BackendCapability, RecoveryDisposition
from kkrunner.client import ExecutionClient, LocalTarget, SshTarget
from kkrunner.backends.config import BackendConfig
from kkrunner.backends.support.class_helper import static_methods
from kkrunner.client.contracts import Operation
from kkrunner.targets.implementations.ssh.rpc import operation_command


MANAGED = frozenset({BackendCapability.REATTACH, BackendCapability.RECOVER, BackendCapability.STATUS,
                     BackendCapability.CANCEL, BackendCapability.INDEPENDENT_OUTCOME})


def test_capabilities_are_immutable_and_instance_specific():
    assert DirectBackend().supports == frozenset({BackendCapability.STATUS, BackendCapability.CANCEL, BackendCapability.REATTACH, BackendCapability.FLEXIBLE_DETACHMENT})
    assert SupervisedBackend().supports == MANAGED | {BackendCapability.SUBMIT_AND_FOLLOW, BackendCapability.ACKNOWLEDGE, BackendCapability.FLEXIBLE_DETACHMENT}
    assert PsijBackend().supports == MANAGED
    assert PsijBackend(executor_name="SLURM").supports == MANAGED
    for name in ("pbs", "local", "flux"):
        assert PsijBackend(executor_name=name).supports == MANAGED - {BackendCapability.RECOVER}
    assert PsijBackend(executor_name="slurm", url="ssh://scheduler").supports == MANAGED - {BackendCapability.RECOVER}


def test_backend_owns_its_options_without_a_second_endpoint_copy():
    backend = PsijBackend(executor_name="pbs")
    for endpoint in (
        ExecutionClient(backend),
        ExecutionClient(backend, target=SshTarget("example.org")),
    ):
        assert endpoint.backend is backend
        assert not hasattr(endpoint, "backend_options")
        assert not hasattr(endpoint, "supports_reattachment")
        assert not hasattr(endpoint, "requires_backend_identity")
        assert endpoint.backend.supports == MANAGED - {BackendCapability.RECOVER}
        assert dict(endpoint.backend.options) == {"executor_name": "pbs", "url": None}
    assert not hasattr(ExecutionClient, "supports_reattachment")
    with pytest.raises(TypeError):
        backend.options["executor_name"] = "slurm"
    with pytest.raises(ValueError, match="existing backend instance"):
        configured_backend(backend, options={"executor_name": "slurm"})


def test_create_backend_from_name_class_or_instance():
    assert isinstance(configured_backend("direct"), DirectBackend)
    assert isinstance(configured_backend(SupervisedBackend), SupervisedBackend)
    backend = PsijBackend(executor_name="pbs")
    assert configured_backend(backend) is backend
    assert isinstance(ExecutionClient("kkrunner").backend, SupervisedBackend)
    assert isinstance(ExecutionClient("psij", target=SshTarget("example")).backend, PsijBackend)
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
        ExecutionClient(backend) if location == "local"
        else ExecutionClient(backend, target=SshTarget("example.org"))
    )
    transport_config = endpoint.target.to_dict()
    backend_config = endpoint.backend_config
    restored = ExecutionClient.from_config(transport_config, backend_config)
    assert isinstance(restored.backend, PsijBackend)
    assert restored.backend is not backend
    assert dict(restored.backend.options) == dict(backend.options)
    assert restored.target.to_dict() == transport_config
    assert restored.backend_config == backend_config
    assert (BackendCapability.RECOVER in restored.backend.supports) is recover
    assert backend_config == BackendConfig(backend_config.name, backend_config.options)
    assert backend_config.supports == backend.supports


@pytest.mark.parametrize("location", ("local", "ssh"))
def test_unsupported_recovery_is_rejected_before_transport_use(location):
    backend = PsijBackend(executor_name="pbs")
    endpoint = (
        ExecutionClient(backend) if location == "local"
        else ExecutionClient(backend, target=SshTarget("invalid.invalid"))
    )
    result = endpoint.recover_submission("irrelevant-key", object())
    assert result.disposition is RecoveryDisposition.UNSUPPORTED


def test_package_layout_and_ssh_operation_are_class_based():
    for kind in ("direct", "kkrunner", "psij"):
        backend_type = backend_class(kind)
        assert backend_type.__module__.endswith(".backend")
        assert static_methods(backend_type)
        operation = Operation.for_backend(backend_type, "stream" if kind == "direct" else "follow")
        command = operation_command(operation, framed="execute" if kind == "direct" else "follow")
        assert "-c" in command
        compile(command[-1], "<SSH backend operation>", "exec")


def test_ssh_dispatch_uses_class_and_request_uses_backend_instance_options():
    backend = PsijBackend(executor_name="pbs")
    local = ExecutionClient(backend)
    ssh = ExecutionClient(backend, target=SshTarget("example.org"))
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


@pytest.mark.parametrize("backend", (DirectBackend, SupervisedBackend, PsijBackend))
def test_backend_configuration_does_not_construct_backend(backend, monkeypatch):
    def unexpected(*args, **kwargs):
        pytest.fail("configuration must not construct a backend")

    expected = backend().supports
    monkeypatch.setattr(backend, "__init__", unexpected)
    config = BackendConfig(backend.name)
    assert dict(config.options) == backend.validate_options()
    assert config.supports == expected


@pytest.mark.parametrize("backend", (DirectBackend, PsijBackend))
def test_backends_without_receipts_do_not_invoke_acknowledgements(backend, monkeypatch):
    client = ExecutionClient(backend, target=SshTarget("invalid.invalid"))

    def unexpected(*args, **kwargs):
        pytest.fail("no receipt to acknowledge")

    monkeypatch.setattr(client, "_invoke", unexpected)
    client.acknowledge(None)
    client.acknowledge_submission(None)


@pytest.mark.parametrize("options", ({"executor_name": ""}, {"url": ""}))
def test_saved_psij_options_are_validated(options):
    with pytest.raises(ValueError, match="nonempty"):
        BackendConfig("psij", options)
