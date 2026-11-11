"""Serializable execution endpoint identity and reconstruction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ExecutionEndpointConfig:
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
    python_command: Any = ("python3",)
    psij_backend: Any = None
    psij_url: Any = None
    ssh_zstd: bool = False
    filesystem_id: str | None = None

    def __post_init__(self):
        object.__setattr__(self, "executor", str(self.executor or "local"))
        if self.filesystem_id is not None and (not isinstance(self.filesystem_id, str) or not self.filesystem_id):
            raise TypeError("filesystem_id must be a nonempty string or None")
        for name in ("ssh_options", "remote_command", "python_command"):
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
                raise TypeError(f"ExecutionEndpointConfig.{name} must be a string or None")
        for name in ("ssh_options", "remote_command", "python_command"):
            if any(not isinstance(item, str) for item in getattr(self, name)):
                raise TypeError(f"ExecutionEndpointConfig.{name} must contain strings")
        if type(self.ssh_zstd) is not bool:
            raise TypeError("ExecutionEndpointConfig.ssh_zstd must be a bool")
        if not self.python_command:
            raise ValueError("ExecutionEndpointConfig.python_command must not be empty")

        # Identity describes the backend actually constructed below. Irrelevant
        # profile fields must not split its pool key or collision namespace.
        if self.executor in ("local", "local-supervised", "psij"):
            for name, value in (
                ("host", None),
                ("ssh_options", ()),
                ("remote_command", ()),
                ("python_command", ("python3",)),
                ("ssh_zstd", False),
            ):
                object.__setattr__(self, name, value)
        if self.executor in ("local", "local-supervised"):
            object.__setattr__(self, "psij_backend", None)
            object.__setattr__(self, "psij_url", None)
        elif self.executor == "psij":
            object.__setattr__(self, "psij_backend", self.psij_backend or "slurm")
        elif self.executor == "ssh-direct":
            # Agent commands and scheduler settings are not part of a direct
            # SSH endpoint's identity or pool key.
            object.__setattr__(self, "remote_command", ())
            object.__setattr__(self, "psij_backend", None)
            object.__setattr__(self, "psij_url", None)
        elif self.executor == "ssh":
            if not self.remote_command:
                object.__setattr__(self, "remote_command", ("kkrunner-agent",))
            if self.psij_backend is not None or self.psij_url is not None:
                object.__setattr__(self, "psij_backend", self.psij_backend or "slurm")


def endpoint_config_for_executor(executor) -> ExecutionEndpointConfig:
    """Describe a live executor without persisting launch/staging policy."""
    from kkrunner.backends.direct import DirectProcessBackend
    from kkrunner.backends.supervised import SupervisedProcessBackend
    from kkrunner.backends.psij import PsijBackend
    from kkrunner.client.agent import AgentRunEndpoint
    from kkrunner.client.ssh_direct import SshDirectRunEndpoint, SshDirectTransport
    from kkrunner.client.transport import SshTransport

    if isinstance(executor, SupervisedProcessBackend):
        return ExecutionEndpointConfig(executor="local-supervised")
    if isinstance(executor, DirectProcessBackend):
        return ExecutionEndpointConfig()
    if isinstance(executor, PsijBackend):
        return ExecutionEndpointConfig(
            executor="psij",
            psij_backend=executor.backend,
            psij_url=executor.url,
        )
    if isinstance(executor, SshDirectRunEndpoint):
        transport = executor.transport
        if not isinstance(transport, SshDirectTransport):
            return ExecutionEndpointConfig(executor="custom:{}.{}".format(
                type(executor).__module__, type(executor).__qualname__,
            ))
        return ExecutionEndpointConfig(
            executor="ssh-direct", host=transport.host, ssh_options=transport.options,
            ssh_zstd=transport.zstd, filesystem_id=executor.filesystem_id,
            python_command=transport.python_command,
        )
    if isinstance(executor, AgentRunEndpoint):
        transport = executor.transport
        psij_backend = None
        psij_url = None
        if executor.backend == "psij":
            options = executor.backend_options or {}
            # ``AgentRunEndpoint(..., backend="psij")`` is a complete
            # public configuration even when no options are supplied: the
            # target-side PSI/J backend then defaults to Slurm. Persist that
            # default explicitly so restart reconstruction cannot silently
            # turn the endpoint into the ordinary remote-local backend.
            psij_backend = options.get("backend") or "slurm"
            psij_url = options.get("url")

        if not isinstance(transport, SshTransport):
            return ExecutionEndpointConfig(
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
        return ExecutionEndpointConfig(
            executor="ssh",
            host=transport.host,
            ssh_options=transport.options,
            ssh_zstd=transport.zstd,
            remote_command=executor.command,
            filesystem_id=executor.filesystem_id,
            python_command=transport.python_command,
            psij_backend=psij_backend,
            psij_url=psij_url,
        )

    # Custom executors can participate in duplicate-submit protection in this
    # process. Reconstruction is intentionally limited to known endpoint types.
    return ExecutionEndpointConfig(
        executor="custom:{}.{}:{}".format(
            type(executor).__module__,
            type(executor).__qualname__,
            executor.name or type(executor).__qualname__,
        )
    )


def executor_from_endpoint(endpoint: ExecutionEndpointConfig):
    """Reconstruct a standard executor from persisted endpoint identity."""
    from kkrunner.backends.direct import DirectProcessBackend
    from kkrunner.backends.supervised import SupervisedProcessBackend
    from kkrunner.backends.psij import PsijBackend
    from kkrunner.client.agent import AgentRunEndpoint
    from kkrunner.client.ssh_direct import SshDirectRunEndpoint, SshDirectTransport
    from kkrunner.client.transport import SshTransport

    if isinstance(endpoint, dict):
        endpoint = ExecutionEndpointConfig(**endpoint)

    if endpoint.executor == "local":
        return DirectProcessBackend()
    if endpoint.executor == "local-supervised":
        return SupervisedProcessBackend()
    if endpoint.executor == "psij":
        return PsijBackend(
            endpoint.psij_backend or "slurm",
            url=endpoint.psij_url,
        )
    if endpoint.executor == "ssh-direct":
        if not endpoint.host:
            raise ValueError("SSH direct execution endpoint requires 'host'")
        return SshDirectRunEndpoint(
            SshDirectTransport(
                endpoint.host, options=endpoint.ssh_options, zstd=endpoint.ssh_zstd,
                python_command=endpoint.python_command,
            ),
            filesystem_id=endpoint.filesystem_id,
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
        return AgentRunEndpoint(
            SshTransport(
                endpoint.host,
                options=endpoint.ssh_options,
                zstd=endpoint.ssh_zstd,
                python_command=endpoint.python_command,
            ),
            filesystem_id=endpoint.filesystem_id,
            **kwargs,
        )
    raise ValueError(f"Cannot reconstruct unsupported execution endpoint {endpoint.executor!r}")
