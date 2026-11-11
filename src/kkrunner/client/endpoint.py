"""Persist location and backend as independent execution choices."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ExecutionEndpointConfig:
    """Hashable location, backend and connection configuration.

    ``location`` selects local or SSH invocation; ``backend`` selects target
    execution. Backend options are stored as sorted immutable key/value pairs
    so the same configuration can serve as an EndpointPool key and JSON data.
    Per-run paths, MPI intent and file policy belong to the execution request.
    """

    location: str = "local"
    backend: str = "direct"
    backend_options: Any = ()
    host: str | None = None
    ssh_options: Any = ()
    python_command: Any = ("python3",)
    ssh_zstd: bool = False
    filesystem_id: str | None = None

    def __post_init__(self):
        if self.location not in ("local", "ssh") and not self.location.startswith("custom:"):
            raise ValueError(f"Unsupported execution location: {self.location!r}")
        from kkrunner.backends import backend_class
        backend = backend_class(self.backend)
        options = dict(backend.default_options)
        options.update(dict(self.backend_options))
        if any(not isinstance(key, str) or not isinstance(value, (str, int, float, bool, type(None)))
               for key, value in options.items()):
            raise TypeError("Backend options must have string keys and scalar JSON values")
        import json
        json.dumps(options, allow_nan=False)
        object.__setattr__(self, "backend_options", tuple(sorted(options.items())))
        for name in ("ssh_options", "python_command"):
            value = getattr(self, name)
            value = () if value is None else (value,) if isinstance(value, str) else tuple(value)
            if any(not isinstance(item, str) for item in value):
                raise TypeError(f"ExecutionEndpointConfig.{name} must contain strings")
            object.__setattr__(self, name, value)
        for name in ("host", "filesystem_id"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, str) or not value):
                raise TypeError(f"ExecutionEndpointConfig.{name} must be a nonempty string or None")
        if type(self.ssh_zstd) is not bool:
            raise TypeError("ExecutionEndpointConfig.ssh_zstd must be a bool")
        if not self.python_command:
            raise ValueError("ExecutionEndpointConfig.python_command must not be empty")
        if self.location == "local":
            for name, value in (("host", None), ("ssh_options", ()),
                                ("python_command", ("python3",)), ("ssh_zstd", False)):
                object.__setattr__(self, name, value)


def describe_endpoint(endpoint) -> ExecutionEndpointConfig:
    """Describe a configured client endpoint without per-run launch policy."""
    from kkrunner.client.local import LocalRunEndpoint
    from kkrunner.client.ssh import SshRunEndpoint
    from kkrunner.client.transport import SshTransport

    if isinstance(endpoint, LocalRunEndpoint):
        return ExecutionEndpointConfig(
            location="local", backend=endpoint.backend.name,
            backend_options=endpoint.backend_options,
        )
    if isinstance(endpoint, SshRunEndpoint):
        transport = endpoint.transport
        location = "ssh" if isinstance(transport, SshTransport) else (
            f"custom:{type(endpoint).__module__}.{type(endpoint).__qualname__}"
        )
        return ExecutionEndpointConfig(
            location=location, backend=endpoint.backend.name,
            backend_options=endpoint.backend_options,
            host=getattr(transport, "host", None),
            ssh_options=getattr(transport, "options", ()),
            python_command=getattr(transport, "python_command", ("python3",)),
            ssh_zstd=getattr(transport, "zstd", False),
            filesystem_id=endpoint.filesystem_id,
        )
    return ExecutionEndpointConfig(
        location=f"custom:{type(endpoint).__module__}.{type(endpoint).__qualname__}",
    )


def endpoint_from_config(config: ExecutionEndpointConfig):
    """Reconstruct the two independent choices from a persisted descriptor."""
    from kkrunner.backends import backend_class
    from kkrunner.client.local import LocalRunEndpoint
    from kkrunner.client.ssh import SshRunEndpoint
    from kkrunner.client.transport import SshTransport

    if isinstance(config, dict):
        config = ExecutionEndpointConfig(**config)
    backend = backend_class(config.backend)
    options = dict(config.backend_options)
    if config.location == "local":
        return LocalRunEndpoint(backend, backend_options=options)
    if config.location == "ssh":
        if not config.host:
            raise ValueError("SSH execution endpoint requires 'host'")
        return SshRunEndpoint(
            SshTransport(config.host, options=config.ssh_options,
                         zstd=config.ssh_zstd, python_command=config.python_command),
            backend=backend, backend_options=options, filesystem_id=config.filesystem_id,
        )
    raise ValueError(f"Cannot reconstruct custom execution endpoint {config.location!r}")
