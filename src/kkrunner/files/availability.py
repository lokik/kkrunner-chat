"""Local artifact discovery shared by process and PSI/J runners.

No transport or execution state lives here. The caller decides whether the
producer's output streams are safe to consider published.
"""

from __future__ import annotations

import os
from dataclasses import replace
from datetime import datetime, timezone

from kkrunner.files.versions import FileVersion
from kkrunner.files.manifest import FileEntry
from kkrunner.support.durability import sync_file
from kkrunner.files.paths import expand_local_candidates
from kkrunner.support.decorators import portable


@portable
def prepare_target_outputs(request):
    """Protect target inputs and caches, then clear stale declared outputs.

    Standalone target-side operation with the same rules for direct-local and
    direct-SSH execution. No kkrunner imports or external helper functions.
    """
    import glob
    import os
    import shutil
    import stat

    streams = {p for p in (request.get("output_file"), request.get("error_file")) if p}
    protected_candidates = [path for path in (request.get("stdin"), request.get("input_file")) if path]
    for declaration in dict.fromkeys((*request.get("input_files", ()), *request.get("cache_files", ()))):
        if declaration is not None:
            protected_candidates.extend(
                glob.glob(declaration, recursive=True) if glob.has_magic(declaration) else (declaration,)
            )
    def directory_entry(path):
        # Resolve parent aliases but retain the name of the directory entry.
        path = os.path.abspath(path)
        return os.path.join(os.path.realpath(os.path.dirname(path)), os.path.basename(path))

    def contains(directory, path):
        return path == directory or path.startswith(directory.rstrip(os.sep) + os.sep)

    protected = set()
    protected_inodes = set()
    protected_directories = set()
    for candidate in dict.fromkeys(protected_candidates):
        real = os.path.realpath(candidate)
        protected.update((real, directory_entry(candidate)))
        try:
            info = os.stat(candidate)
        except FileNotFoundError:
            continue
        protected_inodes.add((info.st_dev, info.st_ino))
        if stat.S_ISDIR(info.st_mode):
            protected_directories.add(real)

    def is_protected(path):
        real = os.path.realpath(path)
        # Also protect the directory entry itself if it is a symlink.
        entry = directory_entry(path)
        if real in protected or entry in protected:
            return True
        if any(contains(root, name)
               for root in protected_directories for name in (real, entry)):
            return True
        try:
            info = os.stat(path)
        except FileNotFoundError:
            return False
        return (info.st_dev, info.st_ino) in protected_inodes

    patterns = request.get("output_files", ())
    directory_outputs = set(request.get("output_directories", ()))
    execution_dir = os.path.normpath(request.get("directory") or os.getcwd())
    for path in streams:
        if is_protected(path):
            raise ValueError("Output stream conflicts with input/cache: " + path)
    # Validate every declaration before deleting anything: a later collision
    # must not leave a partially cleared directory after a preflight failure.
    removable = []
    for declaration in dict.fromkeys(patterns):
        if declaration in directory_outputs:
            normalized = os.path.normpath(declaration)
            if (not os.path.isabs(normalized) or normalized == os.sep
                    or os.path.commonpath((normalized, execution_dir)) == normalized):
                raise ValueError("Refusing to clear a root or working directory: " + declaration)
            cursor = normalized
            while cursor != os.path.dirname(cursor):
                if os.path.islink(cursor):
                    raise ValueError("Directory output crosses a symbolic link: " + declaration)
                cursor = os.path.dirname(cursor)
            if os.path.lexists(normalized) and not os.path.isdir(normalized):
                raise ValueError("Declared directory output is not a directory: " + declaration)
            root = os.path.realpath(normalized)
            if any(contains(root, candidate) for candidate in protected):
                raise ValueError("Directory output contains input/cache: " + declaration)
        # The explicit directory declaration determines the pathname kind.
        # Its spelling may contain literal glob metacharacters.
        wildcard = declaration not in directory_outputs and glob.has_magic(declaration)
        matches = glob.glob(declaration, recursive=True) if wildcard else [declaration]
        for path in matches:
            if path in streams:
                continue
            if is_protected(path):
                if wildcard:
                    continue
                raise ValueError("Explicit output conflicts with input/cache: " + path)
            if path not in directory_outputs and os.path.exists(path) and not wildcard and not (
                os.path.islink(path) or os.path.isfile(path)
            ):
                raise IsADirectoryError(path)
            removable.append(path)

    for path in dict.fromkeys(removable):
        if path in directory_outputs and os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.islink(path) or os.path.isfile(path):
            os.unlink(path)


def discover_local_files(files, *, include_streams=True, require_outputs=True):
    """Resolve pending local declarations and retain individual wildcard matches."""
    declarations = []
    if include_streams:
        declarations.extend(("output", entry) for entry in (files.stdout, files.stderr))
    declarations.extend(("output", entry) for entry in files.outputs)
    declarations.extend(("cache", item.file) for item in files.caches)

    pending = [
        (kind, entry) for kind, entry in declarations
        if entry is not None and entry.collect and entry.local_path is not None
        and entry.source_pattern is None and entry.availability_state in (None, "pending")
    ]
    # Resolve exact artifacts first, as in remote synchronization. A wildcard
    # should never infer availability from an unresolved exact declaration.
    for kind, entry in sorted(pending, key=lambda item: files.is_pattern(item[1])):
        path = entry.local_path
        if files.is_pattern(entry):
            matches = tuple(candidate for candidate in expand_local_candidates(path) if os.path.isfile(candidate))
            collected = tuple(
                concrete for concrete in matches
                if kind != "output" or not files.output_excluded(concrete)
            )
            for concrete in collected:
                # A wildcard must not overwrite an exact artifact's own
                # availability, e.g. a failed or unpublished stdout.
                if any(item.local_path == concrete for item in files.artifacts):
                    continue
                files = files.with_file_availability(
                    concrete,
                    "available",
                    kind=kind,
                    source_pattern=path,
                )
            state = "available" if any(
                item.local_path in collected and item.availability_state == "available"
                for item in files.artifacts
            ) else "skipped"
            if matches and not collected:
                # There were output files, but all were explicitly excluded.
                files = files.with_file_availability(path, state)
                continue
        else:
            exists = os.path.isdir(path) if entry.kind == "directory" else os.path.isfile(path)
            state = "available" if exists else "skipped"
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
    from kkrunner.runs.states import NO_EXECUTION_FAILURES

    if status.failure_phase in NO_EXECUTION_FAILURES:
        return files.skip_pending_artifacts()

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

    return discover_local_files(
        files, include_streams=False,
        require_outputs=status.state == RunState.FINISHED and status.return_code == 0,
    )


def _concrete_result_files(manifests):
    paths = []
    seen = set()
    for manifest in manifests:
        for path in expand_local_candidates(manifest):
            path = os.path.abspath(path)
            if path in seen or not os.path.isfile(path):
                continue
            seen.add(path)
            paths.append(path)
    return tuple(paths)


def _capturable_entries(manifest, entries, *, frozen=False):
    """Only versions actually delivered for this run enter its provenance.

    A skipped auxiliary output can still exist locally from an earlier run.
    Expanded glob declarations likewise must not capture unrelated old local
    matches: concrete discovered outputs have their own manifest entries.
    """
    return tuple(
        entry
        for entry in entries
        if entry is not None and entry.local_path is not None
        and entry.collect
        and entry.availability_state not in ("skipped", "failed")
        and (not frozen or entry.availability_state == "available")
        and (
            not manifest.is_pattern(entry)
            or (not frozen and entry.availability_state in (None, "pending"))
        )
    )


def snapshot_file_entries(entries, *, sync=False, is_pattern=FileEntry.is_pattern):
    """Capture concrete local files and declared directory contents."""
    paths = {}
    for entry in entries:
        if entry.local_path is None:
            continue
        if is_pattern(entry):
            paths.update(dict.fromkeys(_concrete_result_files((entry.local_path,)), "file"))
            continue
        # A published concrete artifact must have a version. If it disappeared
        # or changed kind, strict capture must fail rather than omit it.
        exists = os.path.isdir if entry.kind == "directory" else os.path.isfile
        if entry.availability_state == "available" or exists(entry.local_path):
            paths[os.path.abspath(entry.local_path)] = entry.kind
    versions = []
    for path, kind in sorted(paths.items()):
        if sync and kind == "file":
            sync_file(path)
        versions.append(FileVersion.capture(path, require_digest=True, kind=kind))
    return tuple(versions)


def sync_result_files(manifests):
    """Flush existing files selected by manifest paths before submission."""
    for path in _concrete_result_files(manifests):
        sync_file(path)


def file_is_current(result_files, path, versions):
    """Check a local file against captured versions, or existence before capture."""
    if result_files is None or path is None:
        return False
    path = os.path.abspath(path)
    if result_files.captured_at is None and not versions:
        return os.path.isfile(path)
    for version in versions:
        if os.path.abspath(os.fspath(version.path)) == path:
            return version.is_current()
    return False


def capture_result_files(result_files, *, require_outputs=True, cache_versions=None):
    """Capture published outputs and caches, retaining submission-time inputs."""
    if result_files is None or result_files.captured_at is not None:
        return result_files
    frozen = result_files.remote_patterns_resolved is not None
    if not frozen:
        result_files = discover_local_files(result_files, require_outputs=require_outputs)
    output_entries = (result_files.stdout, result_files.stderr, *result_files.outputs)
    cache_entries = tuple(item.file for item in result_files.caches)
    return replace(
        result_files,
        output_versions=snapshot_file_entries(
            _capturable_entries(result_files, output_entries, frozen=frozen), sync=True,
            is_pattern=result_files.is_pattern,
        ),
        cache_versions=(snapshot_file_entries(
            _capturable_entries(result_files, cache_entries, frozen=frozen),
            is_pattern=result_files.is_pattern,
        ) if cache_versions is None else cache_versions),
        captured_at=datetime.now(timezone.utc).isoformat(),
    )
