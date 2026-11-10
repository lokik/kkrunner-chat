"""Execution-owned output streams, kept separate from file declarations."""

from __future__ import annotations

import os
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from kkrunner.files.paths import file_path


def _close_distinct(resources, *, suppress_errors=False):
    """Close each distinct resource once; keep closing after an error."""
    seen = set()
    first_error = None
    for resource in resources:
        if resource is None or id(resource) in seen:
            continue
        seen.add(id(resource))
        close = getattr(resource, "close", None)
        if close is None or getattr(resource, "closed", False):
            continue
        try:
            close()
        except BaseException as exc:
            if first_error is None:
                first_error = exc
    if first_error is not None and not suppress_errors:
        raise first_error


def close_output_files(*files):
    """Close distinct output handles, including merged stdout/stderr.

    Paths and absent streams need no closing. An error must not leave another
    open output handle behind; propagate the first error after cleanup.
    """
    _close_distinct(files)




@dataclass(frozen=True)
class OutputStreams:
    """Live output handles acquired by a Job adapter or file session."""

    output_file: Any = None
    error_file: Any = None

    def close(self):
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
