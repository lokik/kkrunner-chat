"""Backend operation descriptions and command transport contracts."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Callable, Protocol

@dataclass(frozen=True)
class Operation:
    """Reference a backend method or a standalone function for a transport.

    This is a client-side description, not an object reconstructed on the
    target. A transport validates source delivery only when it needs it.
    """

    method: str | None = None
    function: Callable | None = None
    backend: type | None = None

    def __post_init__(self):
        if (self.function is None) == (self.backend is None):
            raise ValueError("Choose a backend method or a standalone function")
        if self.function is not None:
            if self.method is not None:
                raise ValueError("A standalone operation cannot also name a backend method")
            if not callable(self.function):
                raise TypeError("A standalone operation must be callable")
        else:
            from kkrunner.backends.support.class_helper import static_methods
            if self.method not in static_methods(self.backend):
                raise ValueError(f"{self.backend.__name__}.{self.method} is not a static backend operation")

    @classmethod
    def for_backend(cls, backend: type, method: str):
        """Reference one declared static backend method without an instance."""
        return cls(method=method, backend=backend)

    @classmethod
    def standalone(cls, function: Callable):
        return cls(function=function)


class RunningCommand(Protocol):
    """A running command with independently readable output streams."""

    stdout: asyncio.StreamReader
    stderr: asyncio.StreamReader
    stdin: asyncio.StreamWriter | None
    pid: int
    returncode: int | None

    async def wait(self) -> int: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...


