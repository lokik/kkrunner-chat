"""Local artifact discovery shared by process and PSI/J runners.

No transport or execution state lives here. The caller decides whether the
producer's output streams are safe to consider published.
"""

from __future__ import annotations

import os

from kkrunner.files.paths import expand_local_candidates, has_file_magic


def prepare_target_outputs(request):
    """Protect target inputs and caches, then clear stale declared outputs.

    Standalone target-side operation with the same rules for direct-local and
    direct-SSH execution. No kkrunner imports or external helper functions.
    """
    import glob
    import os

    sync = request.get("client_sync") or {}
    streams = {p for p in (request.get("output_file"), request.get("error_file")) if p}
    protected_declarations = [
        *request.get("input_files", ()),
        request.get("stdin"),
        request.get("input_file"),
        *(item["path"] for item in sync.get("cache_files", ())),
        *sync.get("cache_patterns", ()),
        *(item["path"] for item in sync.get("cache_before", ())),
    ]
    protected = set()
    protected_inodes = set()
    for declaration in protected_declarations:
        if declaration is None:
            continue
        matches = glob.glob(declaration, recursive=True) if glob.has_magic(declaration) else [declaration]
        for candidate in matches:
            protected.add(os.path.realpath(candidate))
            if os.path.exists(candidate):
                info = os.stat(candidate)
                protected_inodes.add((info.st_dev, info.st_ino))

    def is_protected(path):
        if os.path.realpath(path) in protected:
            return True
        if os.path.exists(path):
            info = os.stat(path)
            return (info.st_dev, info.st_ino) in protected_inodes
        return False

    patterns = [*sync.get("output_files", ()), *sync.get("output_patterns", ())]
    for path in streams:
        if is_protected(path):
            raise ValueError("Output stream conflicts with input/cache: " + path)
    for declaration in dict.fromkeys(patterns):
        wildcard = glob.has_magic(declaration)
        matches = glob.glob(declaration, recursive=True) if wildcard else [declaration]
        for path in matches:
            if path in streams:
                continue
            if is_protected(path):
                if wildcard:
                    continue
                raise ValueError("Explicit output conflicts with input/cache: " + path)
            if os.path.islink(path) or os.path.isfile(path):
                os.unlink(path)
            elif os.path.exists(path) and not wildcard:
                raise IsADirectoryError(path)


def discover_local_files(files, *, include_streams=True, require_outputs=True):
    """Resolve pending local declarations and retain individual wildcard matches."""
    declarations = []
    if include_streams:
        declarations.extend(("output", entry) for entry in (files.stdout, files.stderr))
    declarations.extend(("output", entry) for entry in files.outputs)
    declarations.extend(("cache", item.file) for item in files.caches)

    for kind, entry in declarations:
        if (
            entry is None
            or entry.local_path is None
            or entry.source_pattern is not None
            or entry.availability_state not in (None, "pending")
        ):
            continue
        path = entry.local_path
        if has_file_magic(path):
            matches = tuple(candidate for candidate in expand_local_candidates(path) if os.path.isfile(candidate))
            for concrete in matches:
                files = files.with_file_availability(
                    concrete,
                    "available",
                    kind=kind,
                    source_pattern=path,
                )
            state = "available" if matches else "skipped"
        else:
            state = "available" if os.path.isfile(path) else "skipped"
        message = None
        if state == "skipped" and kind == "output" and require_outputs and not entry.optional:
            state, message = "failed", f"Required output file is missing: {path}"
        files = files.with_file_availability(path, state, message)
    return files


def _expanded_protected_paths(protected):
    """Expand protected path declarations once, retaining their source names."""
    for value in protected:
        if value is None:
            continue
        declaration = os.path.abspath(os.fspath(value))
        for candidate in expand_local_candidates(declaration):
            yield declaration, os.path.abspath(candidate)


def validate_output_collisions(streams, *, protected=()):
    """Reject explicit output paths overlapping declared inputs or restart caches.

    Broad output patterns retain their existing behavior: matching protected
    files are skipped by clear_declared_outputs rather than removed.
    """
    protected_paths = tuple(_expanded_protected_paths(protected))
    for stream in streams:
        if stream is None:
            continue
        target = os.path.abspath(os.fspath(stream))
        if has_file_magic(target):
            continue
        for declaration, candidate in protected_paths:
            same_path = os.path.realpath(target) == os.path.realpath(candidate)
            same_inode = os.path.exists(target) and os.path.exists(candidate) and os.path.samefile(target, candidate)
            if same_path or same_inode:
                raise ValueError(f"Output {target} overlaps protected input/cache {declaration}")


def clear_declared_outputs(paths, *, protected=()):
    """Remove old declared results without destroying inputs or restart caches.

    Called on the execution host before starting a new calculation, never on
    reattach. A wildcard removes only its concrete files.
    """
    protected = {candidate for _, candidate in _expanded_protected_paths(protected)}
    for declared in dict.fromkeys(os.fspath(p) for p in paths if p is not None):
        for candidate in expand_local_candidates(declared):
            path = os.path.abspath(candidate)
            if path in protected or not os.path.isfile(path):
                continue
            os.unlink(path)


def local_manifest(files, status):
    """Resolve stream availability, including partial cancelled-run output.

    The supervisor writes streams directly during execution. Cancellation and
    execution failure do not retract already written bytes.
    """
    import os
    from kkrunner.runs.base import FailurePhase, RunState

    published = status.state in (RunState.FINISHED, RunState.CANCELLED) or (
        status.state == RunState.FAILED and status.failure_phase == FailurePhase.EXECUTION
    )
    for entry in (files.stdout, files.stderr):
        if entry is None or entry.local_path is None:
            continue
        if status.failure_phase == FailurePhase.ARTIFACTS:
            state, message = "failed", status.message or "Output publication failed"
        elif published:
            exists = os.path.isfile(entry.local_path)
            state, message = (
                ("available", None)
                if exists
                else (
                    "failed",
                    "Published output file is missing",
                )
            )
        else:
            state, message = "skipped", None
        files = files.with_file_availability(entry.local_path, state, message)

    if status.failure_phase in (FailurePhase.LAUNCH, FailurePhase.SUBMISSION):
        return files
    return discover_local_files(files, include_streams=False, require_outputs=status.state == RunState.FINISHED)


