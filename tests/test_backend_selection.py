"""Backend kinds identify functions, not Python objects or client sessions."""

import pytest

from kkrunner.backends import available_backend_names, installed_backend_names, operation_module, operation_ref
from kkrunner.client.operations import BackendOperation


def test_installed_backend_names_and_remote_service_share_one_registry():
    from kkrunner.remote.service import _backend_for_kind
    assert available_backend_names() == {"direct", "process", "psij"}
    assert installed_backend_names() == {"process", "psij"}
    for name in installed_backend_names():
        module = operation_module(name)
        assert _backend_for_kind(name).__name__ == module
        reference = BackendOperation.installed(*operation_ref(name, "status"))
        assert reference.module == module
        assert reference.function == "status"


def test_registry_rejects_unregistered_backend_or_invalid_operation():
    with pytest.raises(ValueError, match="Unsupported"):
        operation_module("nonexistent")
    with pytest.raises(ValueError, match="function"):
        operation_ref("process", "not-a-function!")



def test_direct_backend_is_available_without_agent_submission():
    from kkrunner.backends import backend_module
    assert backend_module("direct").__name__ == operation_module("direct")
    from kkrunner.remote.service import _backend_for_kind
    with pytest.raises(ValueError, match="installed agent"):
        _backend_for_kind("direct")
