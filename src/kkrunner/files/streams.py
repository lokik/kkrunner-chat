"""Execution-owned output streams, kept separate from file declarations."""

from __future__ import annotations

import os
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from kkrunner.files.paths import file_path


@dataclass(frozen=True)
class OutputStreams:
    """Live output handles acquired by a Job adapter or file session."""

    output_file: Any = None
    error_file: Any = None

    def close(self):
        from kkrunner.runs.base import close_output_files

        close_output_files(self.output_file, self.error_file)


def open_output_streams(spec):
    """Materialize declared targets while leaving the ExecutionSpec unchanged."""
    with ExitStack() as acquired:
        opened = {}
        targets = {}
        for name in ("output_file", "error_file"):
            value = getattr(spec, name)
            if isinstance(value, (str, os.PathLike)):
                path = file_path(value, spec.directory)
                if path not in opened:
                    Path(path).parent.mkdir(parents=True, exist_ok=True)
                    opened[path] = acquired.enter_context(open(path, "wb"))
                value = opened[path]
            targets[name] = value
        streams = OutputStreams(**targets)
        acquired.pop_all()  # The adapter or FileSession now owns the streams.
        return streams
