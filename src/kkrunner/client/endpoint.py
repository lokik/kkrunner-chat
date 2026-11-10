"""Serializable execution endpoint identity and reconstruction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ExecutionEndpoint:
    """Identity of a reusable execution backend.

    The endpoint contains only connection/backend properties. Launch and
    staging policy deliberately live elsewhere so one executor can be shared
    by profiles that differ only in how individual calculations are launched.
    The value is serializable and hashable, so it can also be persisted with a
    run and used as an :class:`ExecutorPool` key after process restart.
    """

    executor: str = "local"
    host: Any = None
    ssh_options: Any = ()
    remote_command: Any = ()
    psij_backend: Any = None
    psij_url: Any = None
    ssh_zstd: bool = False
    filesystem_id: str | None = None

    def __post_init__(self):
        object.__setattr__(self, "executor", str(self.executor or "local"))
        if self.filesystem_id is not None and (not isinstance(self.filesystem_id, str) or not self.filesystem_id):
            raise TypeError("filesystem_id must be a nonempty string or None")
        for name in ("ssh_options", "remote_command"):
            value = getattr(self, name)
            if value is None:
                value = ()
            elif isinstance(value, str):
                value = (value,)
            else:
                value = tuple(value)
            object.__setattr__(self, name, value)

        for name in ("host", "psij_backend", "psij_url"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, str):
                raise TypeError(f"ExecutionEndpoint.{name} must be a string or None")
        for name in ("ssh_options", "remote_command"):
            if any(not isinstance(item, str) for item in getattr(self, name)):
                raise TypeError(f"ExecutionEndpoint.{name} must contain strings")
        if type(self.ssh_zstd) is not bool:
            raise TypeError("ExecutionEndpoint.ssh_zstd must be a bool")

        # Identity describes the backend actually constructed below. Irrelevant
        # profile fields must not split its pool key or collision namespace.
        if self.executor in ("local", "psij"):
            for name, value in (
                ("host", None),
                ("ssh_options", ()),
                ("remote_command", ()),
                ("ssh_zstd", False),
            ):
                object.__setattr__(self, name, value)
        if self.executor == "local":
            object.__setattr__(self, "psij_backend", None)
            object.__setattr__(self, "psij_url", None)
        elif self.executor == "psij":
            object.__setattr__(self, "psij_backend", self.psij_backend or "slurm")
        elif self.executor == "ssh":
            if not self.remote_command:
                object.__setattr__(self, "remote_command", ("kkrunner-agent",))
            if self.psij_backend is not None or self.psij_url is not None:
                object.__setattr__(self, "psij_backend", self.psij_backend or "slurm")


def endpoint_for_backend(executor) -> ExecutionEndpoint:
    """Describe a live executor without persisting launch/staging policy."""
    from kkrunner.backends.process.engine import ProcessBackend
    from kkrunner.backends.psij.backend import PsijBackend
    from kkrunner.client.agent import AgentBackendProxy
    from kkrunner.client.transport import SshTransport

    if isinstance(executor, ProcessBackend):
        return ExecutionEndpoint()
    if isinstance(executor, PsijBackend):
        return ExecutionEndpoint(
            executor="psij",
            psij_backend=executor.backend,
            psij_url=executor.url,
        )
    if isinstance(executor, AgentBackendProxy):
        transport = executor.transport
        psij_backend = None
        psij_url = None
        if executor.backend == "psij":
            options = executor.backend_options or {}
            # ``AgentBackendProxy(..., backend="psij")`` is a complete
            # public configuration even when no options are supplied: the
            # target-side PSI/J backend then defaults to Slurm. Persist that
            # default explicitly so restart reconstruction cannot silently
            # turn the endpoint into the ordinary remote-local backend.
            psij_backend = options.get("backend") or "slurm"
            psij_url = options.get("url")

        if not isinstance(transport, SshTransport):
            return ExecutionEndpoint(
                executor=(
                    "custom:{}.{}:{}".format(
                        type(executor).__module__,
                        type(executor).__qualname__,
                        executor.name or "remote",
                    )
                ),
                host=getattr(transport, "host", None),
                remote_command=executor.command,
                filesystem_id=executor.filesystem_id,
                psij_backend=psij_backend,
                psij_url=psij_url,
            )
        return ExecutionEndpoint(
            executor="ssh",
            host=transport.host,
            ssh_options=transport.options,
            ssh_zstd=transport.zstd,
            remote_command=executor.command,
            filesystem_id=executor.filesystem_id,
            psij_backend=psij_backend,
            psij_url=psij_url,
        )

    # Custom executors can participate in duplicate-submit protection in this
    # process. Reconstruction is intentionally limited to known endpoint types.
    return ExecutionEndpoint(
        executor="custom:{}.{}:{}".format(
            type(executor).__module__,
            type(executor).__qualname__,
            executor.name or type(executor).__qualname__,
        )
    )


def create_backend_from_endpoint(endpoint: ExecutionEndpoint):
    """Reconstruct a standard executor from persisted endpoint identity."""
    from kkrunner.backends.process.engine import ProcessBackend
    from kkrunner.backends.psij.backend import PsijBackend
    from kkrunner.client.agent import AgentBackendProxy
    from kkrunner.client.transport import SshTransport

    if isinstance(endpoint, dict):
        endpoint = ExecutionEndpoint(**endpoint)

    if endpoint.executor == "local":
        return ProcessBackend()
    if endpoint.executor == "psij":
        return PsijBackend(
            endpoint.psij_backend or "slurm",
            url=endpoint.psij_url,
        )
    if endpoint.executor == "ssh":
        if not endpoint.host:
            raise ValueError("SSH execution endpoint requires 'host'")
        kwargs = {}
        if endpoint.remote_command:
            kwargs["command"] = endpoint.remote_command
        if endpoint.psij_backend is not None or endpoint.psij_url is not None:
            kwargs["backend"] = "psij"
            kwargs["backend_options"] = {
                "backend": endpoint.psij_backend or "slurm",
                "url": endpoint.psij_url,
            }
        return AgentBackendProxy(
            SshTransport(
                endpoint.host,
                options=endpoint.ssh_options,
                zstd=endpoint.ssh_zstd,
            ),
            filesystem_id=endpoint.filesystem_id,
            **kwargs,
        )
    raise ValueError(f"Cannot reconstruct unsupported execution endpoint {endpoint.executor!r}")
