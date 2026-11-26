"""Execution-owned output streams, kept separate from file declarations."""

from __future__ import annotations

import os
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from kkrunner.files.paths import file_path


def _close_distinct(resources, *, suppress_errors=False):
    """Close distinct resources with an idempotent close() contract."""
    seen = set()
    first_error = None
    for resource in resources:
        if resource is None or id(resource) in seen:
            continue
        seen.add(id(resource))
        try:
            resource.close()
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
    _close_distinct(file for file in files if not isinstance(file, (str, bytes, os.PathLike, int)))




@dataclass
class OutputStreams:
    """Own output targets from acquisition through final closure.

    Acquiring supplied handles need not create any declared output paths.
    Streaming backends materialize path declarations only when needed.
    """

    output_file: Any = None
    error_file: Any = None

    def open(self, directory):
        """Open still-unmaterialized paths once; preserve merged streams."""
        with ExitStack() as acquired:
            opened = {}
            targets = {}
            for name in ("output_file", "error_file"):
                value = getattr(self, name)
                if isinstance(value, (str, bytes, os.PathLike)):
                    path = file_path(value, directory)
                    if path not in opened:
                        Path(os.fsdecode(path)).parent.mkdir(parents=True, exist_ok=True)
                        opened[path] = acquired.enter_context(open(path, "wb"))
                    value = opened[path]
                targets[name] = value
            self.output_file = targets["output_file"]
            self.error_file = targets["error_file"]
            acquired.pop_all()

    def close(self):
        output, error = self.output_file, self.error_file
        self.output_file = self.error_file = None
        close_output_files(output, error)
