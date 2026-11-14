"""Program-independent file declarations."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Callable, Optional, Union

_FileTemplate = Union[str, os.PathLike, Callable]


@dataclass(frozen=True, eq=False)
class TaskFile(os.PathLike):
    """One task file declaration, or its resolved path with retained metadata.

    Clients resolve their own templates and callbacks before execution.
    ``type`` and ``info`` are opaque client-owned descriptive strings.
    ``kind`` is a filesystem contract: a directory is never inferred from a suffix.
    A pathname may be a template or callback; a callback returning None means
    the file is not declared for that parameter set.  An optional declared
    file may be absent even after successful execution.
    """

    pathname: _FileTemplate
    type: Optional[str] = None
    info: Optional[str] = None
    optional: bool = False
    kind: str = "file"

    def __post_init__(self):
        path = self.pathname
        if not callable(path):
            path = os.fspath(path)
            if not isinstance(path, str):
                raise TypeError("TaskFile pathname must be a string")
            if not path:
                raise ValueError("TaskFile pathname must not be empty")
            object.__setattr__(self, "pathname", path)
        if self.type is not None:
            object.__setattr__(self, "type", str(self.type))
        if self.info is not None and not isinstance(self.info, str):
            raise TypeError("TaskFile info must be a string or None")
        if type(self.optional) is not bool:
            raise TypeError("TaskFile optional must be a bool")
        if self.kind not in ("file", "directory"):
            raise ValueError("TaskFile kind must be 'file' or 'directory'")

    def __fspath__(self):
        if callable(self.pathname):
            raise TypeError("Resolve the TaskFile callback before using its path")
        return self.pathname

    def __str__(self):
        return str(self.pathname)

    def __eq__(self, other):
        if isinstance(other, TaskFile):
            return (self.pathname, self.type, self.info, self.optional, self.kind) == (
                other.pathname, other.type, other.info, other.optional, other.kind,
            )
        if isinstance(other, str):
            return not self.optional and self.type is None and self.info is None and self.kind == "file" and self.pathname == other
        return NotImplemented

    def __hash__(self):
        if not self.optional and self.type is None and self.info is None and self.kind == "file":
            return hash(self.pathname)
        return hash((self.pathname, self.type, self.info, self.optional, self.kind))
