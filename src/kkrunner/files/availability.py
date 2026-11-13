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

    streams = {p for p in (request.get("output_file"), request.get("error_file")) if p}
    protected_declarations = [
        *request.get("input_files", ()),
        request.get("stdin"),
        request.get("input_file"),
        *request.get("cache_files", ()),
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

    patterns = request.get("output_files", ())
    for path in streams:
        if is_protected(path):
            raise ValueError("Output stream conflicts with input/cache: " + path)
    # Validate every declaration before deleting anything: a later collision
    # must not leave a partially cleared directory after a preflight failure.
    removable = []
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
            if os.path.exists(path) and not wildcard and not (
                os.path.islink(path) or os.path.isfile(path)
            ):
                raise IsADirectoryError(path)
            removable.append(path)

    for path in dict.fromkeys(removable):
        if os.path.islink(path) or os.path.isfile(path):
            os.unlink(path)


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


def local_manifest(files, status):
    """Resolve stream availability, including partial cancelled-run output.

    The supervisor writes streams directly during execution. Cancellation and
    execution failure do not retract already written bytes.
    """
    import os
    from kkrunner.execution import FailurePhase, RunState

    published = status.state in (RunState.FINISHED, RunState.CANCELLED) or (
        status.state == RunState.FAILED and status.failure_phase == FailurePhase.EXECUTION
    )
    for entry in (files.stdout, files.stderr):
        if entry is None or entry.local_path is None:
            continue
        if status.failure_phase in (FailurePhase.ARTIFACTS, FailurePhase.OUTPUT):
            state, message = "failed", status.message or "Output publication failed"
        elif status.state == RunState.FINISHED and status.return_code is None:
            # An unobserved exit cannot prove that the stream was published.
            state, message = (
                ("available", None) if os.path.isfile(entry.local_path)
                else ("skipped", None)
            )
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
    return discover_local_files(
        files, include_streams=False,
        require_outputs=status.state == RunState.FINISHED and status.return_code == 0,
    )
