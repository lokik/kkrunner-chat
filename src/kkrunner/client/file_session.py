"""Client-side staging and terminal result-file synchronization.

This session never submits, observes, waits for, or cancels a backend job. A
Run owns the session and invokes it before and after a distinct Job.
"""

from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path

from kkrunner.client.execution_request import target_request
from kkrunner.files.manifest import (
    CacheEntry,
    FileEntry,
    FileManifest,
    FileSyncPolicy,
)
from kkrunner.files.paths import file_path, has_file_magic
from kkrunner.files.queries import directory_digest
from kkrunner.files.streams import OutputStreams
from kkrunner.files.versions import FileVersion, file_digest
from kkrunner.runs.base import ExecutionOutcome, RunState, SubmissionNotStarted
from kkrunner.support.async_utils import run_coro_sync as _run_sync
from kkrunner.targets.contracts import Operation


class FileSession:
    """Own mapped files and local streams for exactly one client Run."""

    def __init__(
        self, context, spec, *, files=None, remote_directory=None, allow_live_streams=False,
    ):
        self.context = context
        self.spec = spec
        self._allow_live_streams = allow_live_streams
        self._files = FileManifest.from_spec(spec) if files is None else files
        self._remote_directory = remote_directory
        self._prepared = None
        self._streams = None
        self._closed = False
        self.result = ExecutionOutcome(
            output_file=self._local_path(spec.output_file),
            input_file=self._local_path(spec.input_file),
            error_file=self._local_path(spec.error_file),
        )

    @property
    def files(self):
        return self._files

    @property
    def remote_directory(self):
        return self._remote_directory

    def _set_file_availability(
        self,
        local,
        state,
        message=None,
        *,
        remote=None,
        kind="output",
        source_pattern=None,
    ):
        self._files = self._files.with_file_availability(
            local,
            state,
            message,
            remote_path=remote,
            kind=kind,
            source_pattern=source_pattern,
        )

    @property
    def is_local(self):
        return self.context.target.is_local

    @property
    def files_are_local(self):
        return self.is_local or self.context.shared_filesystem

    def _local_path(self, value):
        if value is None:
            return None
        try:
            return file_path(value, self.spec.directory, required=not self.is_local)
        except ValueError as exc:
            raise ValueError("Remote execution requires named files") from exc

    async def _stage_file(self, entry, used, *, required=True, uploads=None, remote_digests=None):
        if entry is None:
            return None
        source, target = entry.local_path, entry.remote_path
        previous = used.get(target)
        if previous is not None and previous != source:
            raise ValueError(f"Remote input target collision: {target}")
        if previous is not None:
            return target

        if self.context.shared_filesystem and not os.path.isfile(source):
            if required:
                raise FileNotFoundError(f"Shared input file does not exist: {source}")
            return None
        policy = self.context.input_files
        if policy == FileSyncPolicy.NEVER:
            used[target] = source
            return target

        local_digest = file_digest(source, missing_ok=True)
        remote_digest = (await self.context.target.file_digest(target)
                         if remote_digests is None else remote_digests.get(target))
        if local_digest is None:
            if not required and remote_digest is None:
                return None
            if required:
                if policy == FileSyncPolicy.ALWAYS:
                    raise FileNotFoundError(f"Local input file does not exist: {source}")
                if remote_digest is None:
                    raise FileNotFoundError(f"Input file exists neither locally nor remotely: {source}")

        if policy.should_transfer(local_digest, remote_digest):
            if uploads is None:
                await self.context.target.put_file(source, target)
            else:
                uploads.append((source, target))

        used[target] = source
        return target

    async def _stage_directory(self, entry, used, *, required=True):
        """Apply the input transfer policy to complete directory contents."""
        source, target = entry.local_path, entry.remote_path
        previous = used.get(target)
        if previous is not None:
            if previous != source:
                raise ValueError(f"Remote input target collision: {target}")
            return target
        if self.context.shared_filesystem and not os.path.isdir(source):
            if required:
                raise FileNotFoundError(f"Shared input directory does not exist: {source}")
            return None
        policy = self.context.input_files
        if policy == FileSyncPolicy.NEVER:
            used[target] = source
            return target
        if os.path.lexists(source) and (os.path.islink(source) or not os.path.isdir(source)):
            raise ValueError(f"Directory input is not a regular directory: {source}")
        local_exists = os.path.isdir(source)
        transport = self.context.target
        observed = (await transport.query_files([target]))[0]
        remote_exists = observed["status"] == "ok" and observed["type"] == "directory"
        if observed["status"] == "ok" and not remote_exists:
            raise ValueError(f"Target directory input is not a directory: {target}")
        if required and policy == FileSyncPolicy.ALWAYS and not local_exists:
            raise FileNotFoundError(f"Local input directory does not exist: {source}")
        if local_exists:
            # ALWAYS/MISSING depend only on existence; CHANGED also needs
            # content identity when both trees exist.
            local_digest, remote_digest = True, True if remote_exists else None
            if policy == FileSyncPolicy.CHANGED and remote_exists:
                local_digest = directory_digest(source)
                remote_digest = await transport.call(Operation.standalone(directory_digest), target)
                if local_digest is None or remote_digest is None:
                    raise ValueError(f"Cannot compare directory input contents: {source} -> {target}")
            if policy.should_transfer(local_digest, remote_digest):
                await transport.put_directory(source, target)
                remote_exists = True
        if required and not remote_exists:
            raise FileNotFoundError(f"Required directory exists neither locally nor on target: {source}")
        if not remote_exists:
            return None
        used[target] = source
        return target

    async def _stage_input_pattern(self, entry, used, remote_root, *, required=True):
        matches = self.context.expand_input(entry, remote_root)
        if not matches and required:
            raise FileNotFoundError(f"Local input pattern did not match any files: {entry.local_path}")
        if not matches:
            return
        policy = self.context.input_files
        pending = [match.remote_path for match in matches if match.remote_path not in used]
        digests = {}
        if pending and policy != FileSyncPolicy.NEVER:
            observed = await self.context.target.query_files(pending, digest=True)
            digests = {item["path"]: item.get("digest") for item in observed}
        uploads = []
        for match in matches:
            await self._stage_file(match, used, required=required, uploads=uploads,
                                   remote_digests=digests)
        if uploads:
            await self.context.target.put_files(
                uploads, source_directory=self.context.local_root,
                destination_directory=remote_root,
            )

    async def _prepare_cache_files(self, used, manifest):
        targets = {}
        patterns = []
        pattern_before = {}
        output_policy = self.context.output_files
        remote_root = manifest.mapping.remote_root

        for cache in manifest.caches:
            entry = cache.file
            local = entry.local_path
            if has_file_magic(local):
                patterns.append(entry.remote_path)
                # Cache patterns use the same transfer logic as inputs,
                # but an unmatched cache pattern is optional.
                await self._stage_input_pattern(entry, used, remote_root, required=False)
                continue

            # Cache inputs obey precisely the same staging policy as ordinary
            # inputs, except that a missing local cache is allowed.
            remote = (
                await self._stage_directory(entry, used, required=False)
                if entry.kind == "directory"
                else await self._stage_file(entry, used, required=False)
            )
            if remote is None:
                # A missing cache need not be uploaded, but remains declared:
                # the calculation may create it and it must be collected.
                remote = entry.remote_path

            targets[local] = (remote, None)

        if output_policy != FileSyncPolicy.NEVER and (patterns or targets):
            # Stage all inputs first, then discover and capture all cache
            # baselines in one request, including exact paths and globs.
            exact = {remote for remote, _ in targets.values()}
            entries = await self.context.target.query_files(
                [*patterns, *(remote for remote, _ in targets.values())],
                root=remote_root,
            )
            signatures = {
                item["path"]: (item["size"], item["mtime_ns"])
                for item in entries
                if item["status"] == "ok" and item["type"] == "file"
            }
            directories = {item["path"] for item in entries
                           if item["status"] == "ok" and item["type"] == "directory"}
            for cache in manifest.caches:
                entry = cache.file
                if entry.kind == "directory" and entry.remote_path in directories:
                    digest = await self.context.target.call(
                        Operation.standalone(directory_digest), entry.remote_path,
                    )
                    if digest is None:
                        raise ValueError(f"Cannot identify cache directory contents: {entry.remote_path}")
                    signatures[entry.remote_path] = ("directory", digest)
            targets = {local: (remote, signatures.get(remote)) for local, (remote, _) in targets.items()}
            pattern_before = {remote: before for remote, before in signatures.items() if remote not in exact}

        return targets, tuple(patterns), pattern_before

    async def _sync_output_entry(self, local, remote, required, *, kind="file"):
        """Synchronize one concrete output and record its availability."""
        try:
            available = await self._sync_one_output(local, remote, required, kind=kind)
        except Exception as exc:
            self._set_file_availability(local, "failed", str(exc))
            raise
        self._set_file_availability(
            local,
            "available" if available else "skipped",
        )

    async def _sync_patterns(
        self, patterns, *, kind, required=False, excluded_remote=(), excluded_local=(), seen_remote=None
    ):
        """Expand and inspect a pattern in one query, then transfer exact pairs."""
        remote_root = await self.context._resolved_remote_root()
        selected = seen_remote if seen_remote is not None else set()
        selected.update(excluded_remote)
        cache_before = ({cache.file.remote_path: cache.before for cache in self._files.caches
                         if cache.file.local_path is None} if kind == "cache" else {})
        excluded_local = set(excluded_local)
        transport = self.context.target
        for pattern in patterns:
            local_pattern = os.fspath(self.context._unmap_path(pattern, remote_root))
            pending = []
            downloads = []
            try:
                # Without transfers, matching local copies can still be
                # available (in particular on a shared filesystem).
                compare = kind == "output" or self.context.output_files == FileSyncPolicy.NEVER
                observed = await transport.query_files(
                    [pattern], root=remote_root, digest=compare,
                )
                matches = [item for item in observed
                           if item["status"] == "ok" and item["type"] == "file"]
                if not matches:
                    if required:
                        raise FileNotFoundError(f"Declared remote {kind} pattern did not match any files: {pattern}")
                    self._set_file_availability(local_pattern, "skipped", kind=kind)
                    continue
                for item in matches:
                    remote = item["path"]
                    if remote in selected:
                        continue
                    selected.add(remote)
                    local = os.fspath(self.context._unmap_path(remote, remote_root))
                    if local in excluded_local or (
                        kind == "output" and self._files.output_excluded(remote, remote=True)
                    ):
                        continue
                    self._set_file_availability(
                        local, "pending", remote=remote, kind=kind, source_pattern=local_pattern,
                    )
                    pending.append(local)
                    if not compare:
                        transfer = self._cache_file_changed(
                            local, cache_before.get(remote), (item["size"], item["mtime_ns"]),
                        )
                        available = transfer
                    else:
                        available, transfer = self._output_file_action(
                            local, remote, required, item.get("digest"),
                        )
                    if transfer:
                        downloads.append((remote, local))
                    else:
                        self._set_file_availability(
                            local, "available" if available else "skipped", kind=kind,
                        )
                        pending.pop()
                if downloads:
                    await transport.get_files(
                        downloads, source_directory=remote_root,
                        destination_directory=self.context.local_root,
                    )
                for local in pending:
                    self._set_file_availability(local, "available", kind=kind)
                matched_paths = {item["path"] for item in matches}
                available = any(entry.remote_path in matched_paths and entry.availability_state == "available"
                                for entry in self._files.artifacts)
                self._set_file_availability(local_pattern, "available" if available else "skipped", kind=kind)
            except Exception as exc:
                for local in pending:
                    self._set_file_availability(local, "failed", str(exc), kind=kind)
                self._set_file_availability(local_pattern, "failed", str(exc), kind=kind)
                raise

    async def _sync_output_files(self, required):
        # Primary streams are ordinary output files, not a reconstruction
        # of the live FOLLOW stream. NEVER disables transfers for all outputs;
        # existing identical copies (including shared mounts) remain usable.
        required = required and self.context.output_files != FileSyncPolicy.NEVER
        declarations = tuple(entry for entry in (self._files.stdout, self._files.stderr, *self._files.outputs)
                             if entry is not None and entry.collect and entry.source_pattern is None
                             and entry.remote_path is not None)
        seen_remote = set()
        for entry in declarations:
            if has_file_magic(entry.remote_path) or entry.local_path is None or entry.remote_path in seen_remote:
                continue
            seen_remote.add(entry.remote_path)
            await self._sync_output_entry(
                entry.local_path, entry.remote_path, required and not entry.optional,
                kind=entry.kind,
            )

        primary = {path for path in (self.result.output_file, self.result.error_file) if path is not None}
        # Even uncollected exact declarations must not be collected via a glob.
        seen_remote.update(entry.remote_path for entry in self._files.outputs
                           if entry.source_pattern is None and entry.remote_path is not None
                           and not has_file_magic(entry.remote_path))
        for entry in declarations:
            if has_file_magic(entry.remote_path):
                await self._sync_patterns(
                    (entry.remote_path,), kind="output",
                    required=required and not entry.optional,
                    excluded_local=primary, seen_remote=seen_remote,
                )

    async def _sync_one_output(self, local, remote, required, *, kind="file"):
        policy = self.context.output_files
        if policy == FileSyncPolicy.NEVER and not os.path.exists(local):
            return False
        if kind == "directory":
            entry = (await self.context.target.query_files([remote]))[0]
            if entry["status"] == "missing":
                if required:
                    raise FileNotFoundError(f"Required directory output not created: {remote}")
                return False
            if entry["status"] != "ok" or entry["type"] != "directory":
                raise ValueError(f"Declared directory output is not a directory: {remote}")
            return await self._sync_directory_output(local, remote)
        remote_digest = await self.context.target.file_digest(remote)
        available, transfer = self._output_file_action(local, remote, required, remote_digest)
        if transfer:
            await self.context.target.get_file(remote, local)
        return available

    async def _sync_directory_output(self, local, remote, *, remote_digest=None):
        """Synchronize an existing target directory; reuse an observed digest."""
        policy = self.context.output_files
        if os.path.lexists(local):
            if os.path.islink(local) or not os.path.isdir(local):
                raise ValueError(f"Local directory output is not a directory: {local}")
            if policy != FileSyncPolicy.ALWAYS:
                if remote_digest is None:
                    remote_digest = await self.context.target.call(
                        Operation.standalone(directory_digest), remote,
                    )
                local_digest = directory_digest(local)
                if remote_digest is not None and remote_digest == local_digest:
                    return True
            if policy in (FileSyncPolicy.MISSING, FileSyncPolicy.NEVER):
                return False
        if policy == FileSyncPolicy.NEVER:
            return False
        await self.context.target.get_directory(remote, local)
        return True

    def _output_file_action(self, local, remote, required, remote_digest):
        """Decide availability and transfer using the same exact/pattern policy."""
        if remote_digest is None:
            if required:
                raise FileNotFoundError(f"Declared remote output file was not created: {remote}")
            return False, False
        local_digest = file_digest(local, missing_ok=True)
        if self.context.output_files.should_transfer(remote_digest, local_digest):
            return True, True
        # MISSING can preserve a stale copy which must not become an artifact.
        return local_digest is not None and local_digest == remote_digest, False

    def _cache_file_changed(self, local, before, after):
        return after is not None and after != before and not (
            self.context.output_files == FileSyncPolicy.MISSING
            and file_digest(local, missing_ok=True) is not None
        )

    async def _sync_cache_entry(self, local, remote, before, *, kind="file"):
        """Synchronize one concrete cache using its pre-run signature."""
        try:
            if self.context.output_files == FileSyncPolicy.NEVER:
                available = await self._sync_one_output(local, remote, False, kind=kind)
                self._set_file_availability(local, "available" if available else "skipped", kind="cache")
                return
            if kind == "directory":
                entry = (await self.context.target.query_files([remote]))[0]
                if entry["status"] == "missing":
                    state = "skipped"
                elif entry["status"] == "ok" and entry["type"] == "directory":
                    digest = await self.context.target.call(
                        Operation.standalone(directory_digest), remote,
                    )
                    if digest is None:
                        raise ValueError(f"Cannot identify cache directory contents: {remote}")
                    if ("directory", digest) == before or (
                        self.context.output_files == FileSyncPolicy.MISSING
                        and os.path.isdir(local)
                    ):
                        state = "skipped"
                    else:
                        available = await self._sync_directory_output(local, remote, remote_digest=digest)
                        state = "available" if available else "skipped"
                else:
                    raise ValueError(f"Declared directory cache is not a directory: {remote}")
                self._set_file_availability(local, state, kind="cache")
                return
            after = await self.context.target.file_signature(remote)
            if self._cache_file_changed(local, before, after):
                await self.context.target.get_file(remote, local)
                state = "available"
            else:
                state = "skipped"
        except Exception as exc:
            self._set_file_availability(local, "failed", str(exc), kind="cache")
            raise
        self._set_file_availability(local, state, kind="cache")

    async def _sync_cache_files(self):
        exact = []
        patterns = []
        for cache in self._files.caches:
            entry = cache.file
            if entry.remote_path is None or entry.source_pattern is not None:
                continue
            if has_file_magic(entry.remote_path):
                patterns.append(entry.remote_path)
            elif entry.local_path is not None:
                exact.append(entry.remote_path)
                await self._sync_cache_entry(
                    entry.local_path, entry.remote_path, cache.before, kind=entry.kind,
                )

        if patterns:
            await self._sync_patterns(patterns, kind="cache", excluded_remote=exact)

    def _remote_argv(self, argv, staged):
        out = []
        for arg in argv:
            value = os.fspath(arg)
            # Match the path identity used when staging; never flush command
            # arguments or reinterpret non-path arguments as file handles.
            replacement = staged.get(file_path(value, self.spec.directory))
            out.append(replacement or value)
        return out

    async def _prepare_request(self, remote_directory, manifest):
        used = {}
        primary = next((entry for entry in manifest.inputs
                        if entry.local_path == self.result.input_file), None)
        input_target = await self._stage_file(primary, used)
        stdin_target = await self._stage_file(manifest.stdin, used)
        for entry in manifest.inputs:
            if entry is primary:
                continue
            required = not entry.optional
            if has_file_magic(entry.local_path):
                await self._stage_input_pattern(entry, used, manifest.mapping.remote_root, required=required)
            elif entry.kind == "directory":
                await self._stage_directory(entry, used, required=required)
            else:
                await self._stage_file(entry, used, required=required)

        inputs = dict(used)
        cache_targets, cache_patterns, cache_before = await self._prepare_cache_files(used, manifest)
        staged = {source: target for target, source in used.items()}
        output_targets = [entry.remote_path for entry in manifest.outputs if not has_file_magic(entry.local_path)]
        output_patterns = [entry.remote_path for entry in manifest.outputs if has_file_magic(entry.local_path)]

        output_target = None if manifest.stdout is None else manifest.stdout.remote_path
        error_target = None if manifest.stderr is None else manifest.stderr.remote_path

        request = target_request(
            self.spec, remote_directory,
            transform=lambda args: self._remote_argv(args, staged),
            input_file=input_target, stdin=stdin_target,
            output_file=output_target, error_file=error_target,
            output_files=(*output_targets, *output_patterns),
            output_directories=(entry.remote_path for entry in manifest.outputs if entry.kind == "directory"),
            cache_files=(*(remote for remote, _ in cache_targets.values()), *cache_patterns),
            input_files=used,
        )
        caches = [
            replace(cache, before=cache_targets[cache.file.local_path][1])
            if cache.file.local_path in cache_targets else cache
            for cache in manifest.caches
        ]
        caches.extend(CacheEntry(FileEntry(remote_path=remote), before)
                      for remote, before in cache_before.items())
        if self.context.shared_filesystem:
            from kkrunner.files.availability import snapshot_file_entries
            versions = snapshot_file_entries(manifest.input_entries)
        else:
            versions = await self._target_input_versions(manifest, inputs)
        return request, replace(manifest, caches=tuple(caches), input_versions=versions)

    async def _target_versions(self, paths):
        transport = self.context.target
        files = [target for target, (_, kind) in paths.items() if kind == "file"]
        observed = await transport.query_files(files, digest=True) if files else ()
        digests = {item["path"]: item.get("digest") for item in observed}
        versions = []
        for target, (local, kind) in paths.items():
            digest = digests.get(target)
            if kind == "directory":
                digest = await transport.call(Operation.standalone(directory_digest), target)
            if digest is not None:
                versions.append(FileVersion(local, fingerprint=None, digest=digest, kind=kind))
        return tuple(versions)

    async def _target_input_versions(self, manifest, inputs):
        directories = {entry.local_path for entry in manifest.input_entries if entry.kind == "directory"}
        return await self._target_versions({
            target: (local, "directory" if local in directories else "file")
            for target, local in inputs.items()
        })

    async def _target_cache_versions(self):
        paths = {}
        for cache in self._files.caches:
            entry = cache.file
            if entry.remote_path is None or entry.local_path is None:
                continue
            if has_file_magic(entry.remote_path):
                root = await self.context._resolved_remote_root()
                for target in await self.context.target.expand_files(entry.remote_path, root):
                    paths[target] = (await self.context.unmap_path(target), entry.kind)
            else:
                paths[entry.remote_path] = (entry.local_path, entry.kind)
        return await self._target_versions(paths)

    def _flush_declared_handles(self):
        """Use live descriptors only after the caller has reserved their paths."""
        seen = set()
        for value in (self.spec.input_file, self.spec.stdin, self.spec.output_file,
                      self.spec.error_file, *self.spec.input_files, *self.spec.output_files,
                      *self.spec.cache_files):
            if value is None or isinstance(value, (str, bytes, os.PathLike, int)) or id(value) in seen:
                continue
            seen.add(id(value))
            value.flush()

    async def _prepare_async(self):
        from kkrunner.files.availability import snapshot_file_entries, sync_result_files

        self.spec.require_complete()
        if self.is_local:
            directory, manifest = await self._resolve_footprint()
            from kkrunner.client.execution_request import execution_request
            declaration = self.spec
            if self._allow_live_streams:
                declaration = replace(self.spec, **{
                    name: file_path(getattr(self.spec, name), self.spec.directory)
                    for name in ("stdin", "output_file", "error_file")
                })
            request = execution_request(declaration)
            try:
                self._flush_declared_handles()
                sync_result_files(manifest.input_files)
                return request, replace(manifest, input_versions=snapshot_file_entries(manifest.input_entries))
            except OSError as exc:
                raise SubmissionNotStarted(str(exc)) from exc

        try:
            directory, manifest = await self._resolve_footprint()
            if (manifest.stdout is not None and manifest.stderr is not None
                    and manifest.stdout.local_path == manifest.stderr.local_path):
                raise ValueError("Remote stdout and stderr must have distinct output files")
            self._flush_declared_handles()
            sync_result_files(manifest.input_files)
            await self.context.target.run(("mkdir", "-p", "--", directory.target))
            return await self._prepare_request(directory.target, manifest)
        except Exception as exc:
            # No native calculation has been launched.
            raise SubmissionNotStarted(str(exc)) from exc

    @classmethod
    async def from_attachment(cls, context, attachment):
        """Restore file declarations without opening or creating output files."""
        from kkrunner.client.staging import StagingSpec
        from kkrunner.runs.base import ExecutionSpec

        execution, files = attachment.execution, attachment.files
        if not context.target.is_local and (execution.backend_id is None or execution.directory.remote is None):
            raise ValueError("Remote run lacks backend identity or working directory")
        spec = ExecutionSpec(
            argv=None, directory=execution.directory.local, input_file=files.input_file,
            output_file=files.output_file, error_file=None if files.stderr is None else files.stderr.local_path,
            staging=StagingSpec.from_mapping(files.mapping),
        )
        return cls(context, spec, files=files, remote_directory=execution.directory.target)

    @property
    def working_directory(self):
        from kkrunner.runs.model import WorkingDirectory
        return WorkingDirectory(os.path.abspath(self.spec.directory),
                                None if self.is_local else self._remote_directory)

    async def _resolve_footprint(self):
        if self._remote_directory is None:
            target = await self.context.map_directory(self.spec.directory)
            self._files = await self.context.map_manifest(self._files)
            self._remote_directory = target
        return self.working_directory, self._files

    def submission_footprint(self):
        """Resolve names once, without creating or transferring files."""
        return _run_sync(self._resolve_footprint())

    def prepare_submission(self):
        """Stage files before backend submit; persistent Run records the manifest."""
        if self._prepared is None:
            # Run has acquired its reservation before entering this method.
            # Borrowed output handles become owned together, before any flush
            # or other file use. Path targets are opened only when needed.
            if self._streams is None:
                self._streams = OutputStreams(self.spec.output_file, self.spec.error_file)
            request, manifest = _run_sync(self._prepare_async())
            self._prepared = request
            self._remote_directory = request["directory"]
            self._files = manifest

    def prepare_job(self):
        """Return the staged request; remote output is owned by the backend."""
        if self._prepared is None:
            raise SubmissionNotStarted("Files must be staged before preparing the job")
        if not self.is_local:
            # Release application-supplied handles before the remote producer
            # starts and before any eventual atomic download replaces a path.
            self.close()
        return dict(self._prepared)

    def local_call_io(self, detached=False):
        """Borrow local live streams for an in-process backend invocation."""
        if detached and any(
            value is not None and not isinstance(value, (str, os.PathLike))
            for value in (self.spec.output_file, self.spec.error_file)
        ):
            raise ValueError("Detached output requires a named file or None")
        self._streams.open(self.spec.directory)
        files = {kind: stream for kind, stream in (
            ("out", self._streams.output_file), ("err", self._streams.error_file),
        ) if stream is not None}
        stdin = self.spec.stdin
        if isinstance(stdin, (str, os.PathLike)):
            stdin = None  # The target operation opens its declared pathname.
        return files, stdin

    @property
    def stream_paths(self):
        return (
            None if self._files.stdout is None else (self._files.stdout.local_path if self.is_local else self._files.stdout.remote_path),
            None if self._files.stderr is None else (self._files.stderr.local_path if self.is_local else self._files.stderr.remote_path),
        )

    def observation(self):
        """A new observer replays named streams independently of local files."""
        return {
            "stream_offsets": {
                "out": None if self.spec.is_partial and self.spec.output_file is None else 0,
                "err": None if self.spec.is_partial and self.spec.error_file is None else 0,
            },
            "stream_paths": self.stream_paths,
        }

    async def _synchronize(self, *, required):
        """Synchronize target-side artifacts for a remote session."""
        try:
            await self._sync_output_files(required=required)
            await self._sync_cache_files()
        except Exception as exc:
            self.mark_transfer_failed(str(exc))
            raise
        for entry in self._files.artifacts:
            if entry.availability_state == "pending":
                self._set_file_availability(entry.local_path, "available")

    def mark_transfer_failed(self, message):
        self._files = self._files.fail_pending_artifacts(message)

    async def finalize_async(self, status):
        """Finalize owned files on the existing event loop, if any."""
        if not status.state.is_terminal:
            raise RuntimeError("Cannot finalize files before terminal backend status")
        from kkrunner.runs.states import NO_EXECUTION_FAILURES

        self.close()
        if status.failure_phase in NO_EXECUTION_FAILURES:
            self._files = self._files.skip_pending_artifacts()
            return self._files
        required = status.state == RunState.FINISHED and status.return_code == 0
        if self.files_are_local:
            from kkrunner.files.availability import local_manifest
            self._files = local_manifest(self._files, status)
            if required and self._files.failed_artifacts:
                raise FileNotFoundError(self._files.availability_message)
        else:
            await self._synchronize(required=required)
        from kkrunner.files.availability import capture_result_files

        if self._files.captured_at is None:
            self._files = capture_result_files(
                self._files, require_outputs=required,
                cache_versions=None if self.files_are_local else await self._target_cache_versions(),
            )
        return self._files

    def finalize(self, status):
        """Synchronously finalize using the same file authority as SSH recovery."""
        return _run_sync(self.finalize_async(status))

    def close(self):
        """Release owned streams once, even if cleanup itself raises."""
        if self._closed:
            return
        self._closed = True
        streams, self._streams = self._streams, None
        if streams is not None:
            streams.close()

