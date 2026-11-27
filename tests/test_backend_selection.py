"""Backend kinds resolve to static operation classes, without a registry."""

import pytest

from kkrunner.backends import available_backend_names, installed_backend_names, backend_class
from kkrunner.backends.support.class_helper import static_methods
from kkrunner.client.contracts import Operation


def test_backend_selection_matches_target_capabilities():
    from kkrunner.remote.service import _backend_for_kind

    assert available_backend_names() == {"direct", "kkrunner", "psij"}
    assert installed_backend_names() == {"kkrunner", "psij"}
    for kind in installed_backend_names():
        cls = backend_class(kind)
        assert _backend_for_kind(kind) is cls
        assert "status" in static_methods(cls)
        operation = Operation.for_backend(cls, "status")
        assert operation.backend is cls
        assert operation.method == "status"


def test_backend_selection_rejects_unknown_kind_or_invalid_operation():
    with pytest.raises(ValueError, match="Unsupported"):
        backend_class("nonexistent")
    with pytest.raises(ValueError, match="static backend operation"):
        Operation.for_backend(backend_class("kkrunner"), "not-a-function!")


def test_removed_process_backend_name_is_rejected():
    from kkrunner.backends.config import BackendConfig
    from kkrunner.remote.service import _backend_for_kind

    with pytest.raises(ValueError, match="Unsupported"):
        backend_class("process")
    saved = BackendConfig("process")
    assert not saved.available
    with pytest.raises(ValueError, match="Unsupported"):
        saved.create()
    with pytest.raises(ValueError, match="installed agent"):
        _backend_for_kind("process")


def test_direct_backend_is_portable_without_agent():
    from kkrunner.remote.service import _backend_for_kind
    from kkrunner.targets.implementations.ssh.helpers import ssh_function
    direct = backend_class("direct")
    assert ssh_function(Operation.for_backend(direct, "status")) is direct.status
    with pytest.raises(ValueError, match="installed agent"):
        _backend_for_kind("direct")
