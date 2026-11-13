"""Client-side staging, stream persistence and result-file synchronization.

This session never submits, observes, waits for, or cancels a backend job. A
Run owns the session and invokes it before and after a distinct Job.
"""

from __future__ import annotations

import os
from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path

from kkrunner.client.execution_request import command_request
from kkrunner.files.manifest import (
    CacheEntry,
    FileEntry,
    FileManifest,
    FileSyncPolicy,
)
from kkrunner.files.paths import file_path, has_file_magic
from kkrunner.files.queries import expand_local_files
from kkrunner.files.streams import OutputStreams, open_output_streams
from kkrunner.files.versions import file_digest
from kkrunner.runs.base import ExecutionOutcome, RunState, SubmissionNotStarted
from kkrunner.support.async_utils import run_coro_sync as _run_sync
from kkrunner.support.file_locks import FileLock


class FileSession:
    """Own mapped files and local streams for exactly one client Run."""

    def __init__(
        self, context, spec, *, files=None, remote_directory=None,
    ):
        self.context = context
        self.spec = spec
        self._files = FileManifest.from_spec(spec) if files is None else files
        self._remote_directory = remote_directory
        self._prepared = None
        self._streams = None
        self._closed = False
        self._staging = {} if files is None else None
        self._stream_positions = {}
        self._retrieve_primary_streams = False
        self.result = ExecutionOutcome(
            output_file=self._local_path(spec.output_file),
            input_file=self._local_path(spec.input_file),
            error_file=self._local_path(spec.error_file),
        )

    def _declarations(self, kind):
        if kind == "cache":
            return [(item.file, item.before) for item in self._files.caches]
        return [(entry, None) for entry in self._files.outputs]

    def _targets(self, kind):
        if self._staging is not None:
            return self._staging.get(kind + "_targets", {})
        return {
            entry.local_path: (entry.remote_path, before) if kind == "cache" else entry.remote_path
            for entry, before in self._declarations(kind)
            if entry.local_path is not None and entry.remote_path is not None
            and entry.source_pattern is None and not has_file_magic(entry.remote_path)
        }

    def _patterns(self, kind):
        if self._staging is not None:
            return self._staging.get(kind + "_patterns", ())
        return tuple(entry.remote_path for entry, _ in self._declarations(kind)
                     if entry.remote_path is not None and entry.source_pattern is None
                     and has_file_magic(entry.remote_path))

    @property
    def _output_targets(self):
        return self._targets("output")

    @_output_targets.setter
    def _output_targets(self, value):
        self._staging["output_targets"] = value

    @property
    def _cache_targets(self):
        return self._targets("cache")

    @_cache_targets.setter
    def _cache_targets(self, value):
        self._staging["cache_targets"] = value

    @property
    def _output_patterns(self):
        return self._patterns("output")

    @_output_patterns.setter
    def _output_patterns(self, value):
        self._staging["output_patterns"] = value

    @property
    def _cache_patterns(self):
        return self._patterns("cache")

    @_cache_patterns.setter
    def _cache_patterns(self, value):
        self._staging["cache_patterns"] = value

    @property
    def _cache_before(self):
        if self._staging is not None:
            return self._staging.get("cache_before", {})
        return {entry.remote_path: before for entry, before in self._declarations("cache")
                if entry.local_path is None}

    @_cache_before.setter
    def _cache_before(self, value):
        self._staging["cache_before"] = value

    def configure_stream_retrieval(self, required):
        """Choose file retrieval when the job does not relay primary streams."""
        if type(required) is not bool:
            raise TypeError("Stream retrieval policy must be bool")
        self._retrieve_primary_streams = required

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
        if self._files is not None:
            self._files = self._files.with_file_availability(
                local,
                state,
                message,
                remote_path=remote,
                kind=kind,
                source_pattern=source_pattern,
            )

    def _set_pending_failed(self, message):
        """Record a transfer-wide failure on still-pending output artifacts."""
        if self._files is None:
            return
        for entry in self._files.artifacts:
            if entry.availability_state in (None, "pending"):
                self._set_file_availability(entry.local_path, "failed", message)

    def _write_output(self, data, kind):
        """Persist FOLLOW bytes at their logical offset, not blindly append.

        Multiple clients may follow the same run into the same local stream.
        Hold a cross-process lock while comparing overlapping bytes and
        extending only the missing suffix. A sidecar avoids changing the
        contents of the stream on platforms with byte-range file locks.
        """
        streams = self._streams
        target = (
            streams.error_file if kind == "err" else streams.output_file
        ) if streams is not None else None
        if target is None or not data:
            return
        offset = self._stream_positions[kind]
        target.flush()
        path = Path(self._local_path(target))
        lock_path = path.with_name(f".{path.name}.follow.lock")
        with FileLock(lock_path), path.open("r+b", buffering=0) as stream:
            stream.seek(0, os.SEEK_END)
            size = stream.tell()
            if size < offset:
                raise RuntimeError(f"FOLLOW stream was truncated: {path}")
            overlap = min(size - offset, len(data))
            if overlap:
                stream.seek(offset)
                if stream.read(overlap) != data[:overlap]:
                    raise RuntimeError(f"Conflicting FOLLOW bytes at offset {offset}: {path}")
            if overlap < len(data):
                stream.seek(offset + overlap)
                stream.write(data[overlap:])
                stream.flush()
        self._stream_positions[kind] = offset + len(data)

    @property
    def is_local(self):
        return self.context.transport.is_local

    def _local_path(self, value):
        if value is None:
            return None
        try:
            return file_path(value, self.spec.directory, flush=True, required=not self.is_local)
        except ValueError as exc:
            raise ValueError("Remote execution requires named files") from exc

    async def _stage_file(self, value, used, *, uploads=None, required=True):
        source = self._local_path(value)
        if source is None:
            return None
        target = await self.context.map_path(source)
        previous = used.get(target)
        if previous is not None and previous != source:
            raise ValueError(f"Remote input target collision: {target}")
        if previous is not None:
            return target

        policy = self.context.input_files
        if policy == FileSyncPolicy.NEVER:
            used[target] = source
            return target

        local_digest = file_digest(source, missing_ok=True)
        remote_digest = await self.context.transport.file_digest(target)
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
                await self.context.transport.put_file(source, target)
            else:
                uploads.append(source)

        used[target] = source
        return target

    async def _stage_input_pattern(self, value, used, *, required=True):
        source = self._local_path(value)
        matches = expand_local_files(source, self.context.local_root)
        if not matches and required:
            raise FileNotFoundError(f"Local input pattern did not match any files: {source}")
        uploads = []
        for match in matches:
            await self._stage_file(
                match,
                used,
                uploads=uploads,
                required=required,
            )
        if uploads:
            await self.context.transport.put_files(
                uploads,
                self.context.local_root,
                await self.context._resolved_remote_root(),
            )

    async def _prepare_cache_files(self, used, manifest):
        targets = {}
        patterns = []
        pattern_before = {}
        output_policy = self.context.output_files
        remote_root = await self.context._resolved_remote_root()

        for cache in manifest.caches:
            local = cache.file.local_path
            if has_file_magic(local):
                remote_pattern = await self.context.map_path(local)
                patterns.append(remote_pattern)
                # Cache patterns use the same transfer logic as inputs,
                # but an unmatched cache pattern is optional.
                await self._stage_input_pattern(local, used, required=False)
                continue

            # Cache inputs obey precisely the same staging policy as ordinary
            # inputs, except that a missing local cache is allowed.
            remote = await self._stage_file(local, used, required=False)
            if remote is None:
                # A missing cache need not be uploaded, but remains declared:
                # the calculation may create it and it must be collected.
                remote = await self.context.map_path(local)

            targets[local] = (remote, None)

        if output_policy != FileSyncPolicy.NEVER and (patterns or targets):
            # Stage all inputs first, then discover and capture all cache
            # baselines in one request, including exact paths and globs.
            exact = {remote for remote, _ in targets.values()}
            entries = await self.context.transport.query_files(
                [*patterns, *(remote for remote, _ in targets.values())],
                root=remote_root,
            )
            signatures = {
                item["path"]: (item["size"], item["mtime_ns"])
                for item in entries
                if item["status"] == "ok" and item["type"] == "file"
            }
            targets = {local: (remote, signatures.get(remote)) for local, (remote, _) in targets.items()}
            pattern_before = {remote: before for remote, before in signatures.items() if remote not in exact}

        self._cache_targets = targets
        self._cache_patterns = tuple(patterns)
        self._cache_before = pattern_before

    async def _prepare_output_files(self, manifest):
        targets = {}
        patterns = []

        for entry in manifest.outputs:
            local = entry.local_path
            remote = await self.context.map_path(local)
            if has_file_magic(local):
                patterns.append(remote)
            else:
                targets[local] = remote
        # Cleanup belongs to the target-side submit operation, where input
        # and cache wildcard protections are resolved together. Never delete
        # remote files during client-side staging.
        self._output_targets = targets
        self._output_patterns = tuple(patterns)

    async def _sync_output_entry(self, local, remote, required, *, source_pattern=None):
        """Synchronize one concrete output and record its availability."""
        if source_pattern is not None:
            self._set_file_availability(
                local,
                "pending",
                remote=remote,
                source_pattern=source_pattern,
            )
        try:
            available = await self._sync_one_output(local, remote, required)
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
        """Expand declarations once; the entry handlers own transfer policy."""
        remote_root = await self.context._resolved_remote_root()
        selected = seen_remote if seen_remote is not None else set()
        selected.update(excluded_remote)
        cache_before = self._cache_before if kind == "cache" else {}
        excluded_local = set(excluded_local)
        for pattern in patterns:
            local_pattern = os.fspath(await self.context.unmap_path(pattern))
            try:
                matches = await self.context.transport.expand_files(pattern, remote_root)
                if not matches:
                    if required:
                        raise FileNotFoundError(f"Declared remote {kind} pattern did not match any files: {pattern}")
                    self._set_file_availability(local_pattern, "skipped", kind=kind)
                    continue
                for remote in matches:
                    if remote in selected:
                        continue
                    selected.add(remote)
                    local = os.fspath(await self.context.unmap_path(remote))
                    if local in excluded_local:
                        continue
                    if kind == "cache":
                        await self._sync_cache_entry(
                            local,
                            remote,
                            cache_before.get(remote),
                            source_pattern=local_pattern,
                        )
                    else:
                        await self._sync_output_entry(
                            local,
                            remote,
                            required,
                            source_pattern=local_pattern,
                        )
                self._set_file_availability(local_pattern, "available", kind=kind)
            except Exception as exc:
                self._set_file_availability(
                    local_pattern,
                    "failed",
                    str(exc),
                    kind=kind,
                )
                raise

    async def _sync_output_files(self, required):
        policy = self.context.output_files
        if policy == FileSyncPolicy.NEVER:
            # FOLLOW transfers primary streams even when separate artifact
            # synchronization is disabled.
            primary = {path for path in (self.result.output_file, self.result.error_file) if path is not None}
            for entry in () if self._files is None else self._files.artifacts:
                if entry.availability_state == "pending":
                    self._set_file_availability(
                        entry.local_path,
                        "available" if entry.local_path in primary else "skipped",
                    )
            return

        # The persisted manifest is the sole authority for which declared
        # outputs may be absent. This also works on historical attach, whose
        # reconstructed ExecutionSpec has no output declarations.
        optional = {entry.remote_path: entry.optional for entry in self._files.outputs}
        for local, remote in self._output_targets.items():
            await self._sync_output_entry(
                local,
                remote,
                required and not optional.get(remote, False),
            )

        if self._output_patterns:
            primary = {path for path in (self.result.output_file, self.result.error_file) if path is not None}
            # All output patterns share one transfer set, even when their
            # required/optional policies differ. Exact outputs seed it.
            seen_remote = set(self._output_targets.values())
            for pattern in self._output_patterns:
                await self._sync_patterns(
                    (pattern,),
                    kind="output",
                    required=required and not optional.get(pattern, False),
                    excluded_local=primary,
                    seen_remote=seen_remote,
                )

    async def _sync_one_output(self, local, remote, required):
        policy = self.context.output_files
        remote_digest = await self.context.transport.file_digest(remote)
        if remote_digest is None:
            if required:
                raise FileNotFoundError(f"Declared remote output file was not created: {remote}")
            return False
        local_digest = file_digest(local, missing_ok=True)
        if policy.should_transfer(remote_digest, local_digest):
            await self.context.transport.get_file(remote, local)
        return True

    async def _sync_cache_entry(self, local, remote, before, *, source_pattern=None):
        """Synchronize one concrete cache using its pre-run signature."""
        if source_pattern is not None:
            self._set_file_availability(
                local,
                "pending",
                remote=remote,
                kind="cache",
                source_pattern=source_pattern,
            )
        try:
            after = await self.context.transport.file_signature(remote)
            if (
                after is None
                or after == before
                or (
                    self.context.output_files == FileSyncPolicy.MISSING
                    and file_digest(local, missing_ok=True) is not None
                )
            ):
                state = "skipped"
            else:
                await self.context.transport.get_file(remote, local)
                state = "available"
        except Exception as exc:
            self._set_file_availability(local, "failed", str(exc), kind="cache")
            raise
        self._set_file_availability(local, state, kind="cache")

    async def _sync_cache_files(self):
        if self.context.output_files == FileSyncPolicy.NEVER:
            return

        for local, (remote, before) in self._cache_targets.items():
            await self._sync_cache_entry(local, remote, before)

        if self._cache_patterns:
            await self._sync_patterns(
                self._cache_patterns,
                kind="cache",
                excluded_remote=(remote for remote, _ in self._cache_targets.values()),
            )

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
        # The declared manifest merges aliases with required-wins semantics.
        # Stage the primary input and stdin first: an optional auxiliary alias
        # must never weaken a mandatory stream.
        input_requirements = {entry.local_path: not entry.optional for entry in manifest.inputs}
        input_target = await self._stage_file(self.spec.input_file, used)
        stdin_target = await self._stage_file(self.spec.stdin, used)
        for value in self.spec.input_files:
            source = self._local_path(value)
            required = input_requirements.get(source, True)
            if source is not None and has_file_magic(source):
                await self._stage_input_pattern(value, used, required=required)
            else:
                await self._stage_file(value, used, required=required)

        await self._prepare_cache_files(used, manifest)
        staged = {source: target for target, source in used.items()}
        await self._prepare_output_files(manifest)

        output_target = await self.context.map_path(self.result.output_file) if self.result.output_file else None
        error_target = await self.context.map_path(self.result.error_file) if self.result.error_file else None

        return {
            "directory": remote_directory,
            **command_request(self.spec, lambda args: self._remote_argv(args, staged)),
            "input_file": input_target,
            "stdin": stdin_target,
            "output_file": output_target,
            "error_file": error_target,
            "output_files": [*self._output_targets.values(), *self._output_patterns],
            "cache_files": [*(remote for remote, _ in self._cache_targets.values()), *self._cache_patterns],
            "input_files": list(used.keys()),
        }

    async def _submitted_manifest(self, request, local):
        mapped = await self.context.map_manifest(local)
        caches = []
        for cache in mapped.caches:
            previous = self._cache_targets.get(cache.file.local_path)
            caches.append(replace(cache, before=None if previous is None else previous[1]))
        for remote, before in self._cache_before.items():
            caches.append(CacheEntry(FileEntry(remote_path=remote), before))

        return replace(
            mapped,
            caches=tuple(caches),
            stdout=None
            if mapped.stdout is None
            else replace(
                mapped.stdout,
                remote_path=request.get("output_file"),
            ),
            stderr=None
            if mapped.stderr is None
            else replace(
                mapped.stderr,
                remote_path=request.get("error_file"),
            ),
        )

    async def _prepare_async(self):
        if self.is_local:
            from kkrunner.client.execution_request import execution_request
            self.spec.require_complete()
            manifest = FileManifest.from_spec(self.spec)
            declaration = self.spec
            if self.context.client.backend.execution_mode == "direct":
                declaration = replace(self.spec, **{
                    name: file_path(getattr(self.spec, name), self.spec.directory)
                    for name in ("stdin", "output_file", "error_file")
                })
            return execution_request(declaration), manifest
        try:
            # Reject invalid file declarations *before* creating target folders
            # or uploading anything. The same validated manifest then owns
            # staging requirements and the persisted submission snapshot.
            local = FileManifest.from_spec(self.spec)
            if (
                local.stdout is not None and local.stderr is not None
                and local.stdout.local_path == local.stderr.local_path
            ):
                # The remote FOLLOW protocol carries two independent byte
                # offsets. They cannot reproduce the order of two writers
                # sharing one physical stream file without a merged protocol.
                raise ValueError("Remote stdout and stderr must have distinct output files")
            remote_directory = await self.context.map_directory(self.spec.directory)
            await self.context.transport.run(("mkdir", "-p", "--", remote_directory))
            request = await self._prepare_request(remote_directory, local)
            manifest = await self._submitted_manifest(request, local)
            return request, manifest
        except Exception as exc:
            # No calculation has been launched yet. Persisted preflight and
            # direct submission must classify this known failure identically.
            raise SubmissionNotStarted(str(exc)) from exc

    @classmethod
    async def from_attachment(cls, context, attachment):
        """Open local streams and restore transfer baselines from FileManifest."""
        execution, files = attachment.execution, attachment.files
        if context.transport.is_local:
            from kkrunner.runs.base import ExecutionSpec
            spec = ExecutionSpec(argv=None, directory=execution.directory.local,
                                 input_file=files.input_file, output_file=files.output_file,
                                 error_file=None if files.stderr is None else files.stderr.local_path)
            return cls(context, spec, files=files, remote_directory=execution.directory.target)
        if execution.backend_id is None or execution.directory.remote is None:
            raise ValueError("Remote run lacks backend identity or working directory")
        directory = Path(execution.directory.local)
        directory.mkdir(parents=True, exist_ok=True)
        output_local = files.output_file
        error_local = None if files.stderr is None else files.stderr.local_path
        with ExitStack() as streams:
            output = None if output_local is None else streams.enter_context(open(output_local, "ab"))
            error = (
                None if error_local is None
                else output if error_local == output_local
                else streams.enter_context(open(error_local, "ab"))
            )
            from kkrunner.client.staging import StagingSpec
            from kkrunner.runs.base import ExecutionSpec
            spec = ExecutionSpec(
                argv=None,
                input_file=files.input_file,
                output_file=output_local,
                error_file=error_local,
                directory=os.fspath(directory),
                staging=StagingSpec(
                    local_root=os.fspath(context.local_root),
                    remote_root=context.remote_root,
                    input_sync=context.input_files.value,
                    output_sync=context.output_files.value,
                    filesystem_id=context.filesystem_id,
                ),
            )
            session = cls(
                context, spec, files=files,
                remote_directory=execution.directory.remote,
            )
            session._streams = OutputStreams(output, error)
            streams.pop_all()  # Explicitly transfer stream ownership.
            return session

    def _working_directory(self, target):
        from kkrunner.runs.model import WorkingDirectory
        return WorkingDirectory(os.path.abspath(self.spec.directory), None if self.is_local else target)

    def submission_footprint(self, files):
        """Resolve names for reservation without creating or transferring files."""
        async def resolve():
            directory = await self.context.map_directory(self.spec.directory)
            return self._working_directory(directory), await self.context.map_manifest(files)

        return _run_sync(resolve())

    def execution(self, backend_id):
        if backend_id is None and self._remote_directory is None:
            return None
        from kkrunner.client.transports import require_persistable_transport
        from kkrunner.runs.model import ExecutionMetadata
        return ExecutionMetadata(
            require_persistable_transport(self.context.transport),
            self._working_directory(self._remote_directory),
            backend=self.context.client.backend_config,
            backend_id=backend_id,
        )

    def prepare_submission(self):
        """Stage files before backend submit; persistent Run records the manifest."""
        if self._prepared is None:
            request, manifest = _run_sync(self._prepare_async())
            self._prepared = request
            self._remote_directory = request["directory"]
            self._files = manifest
            self._staging = None

    def prepare_job(self):
        """Open locally owned output streams and return a backend request."""
        self.prepare_submission()
        request = self._prepared
        if not self.is_local and self._streams is None:
            try:
                self._streams = open_output_streams(self.spec)
            except Exception as exc:
                raise SubmissionNotStarted(str(exc)) from exc
        return dict(request)

    def local_call_io(self, detached=False):
        """Borrow local live streams for an in-process backend invocation."""
        if detached and any(
            value is not None and not isinstance(value, (str, os.PathLike))
            for value in (self.spec.output_file, self.spec.error_file)
        ):
            raise ValueError("Detached output requires a named file or None")
        if self._streams is None:
            self._streams = open_output_streams(self.spec)
        files = {kind: stream for kind, stream in (
            ("out", self._streams.output_file), ("err", self._streams.error_file),
        ) if stream is not None}
        stdin = self.spec.stdin
        if isinstance(stdin, (str, os.PathLike)):
            stdin = None  # The target operation opens its declared pathname.
        return files, stdin

    @property
    def stream_paths(self):
        if self._files is None:
            return (None, None)
        return (
            None if self._files.stdout is None else (self._files.stdout.local_path if self.is_local else self._files.stdout.remote_path),
            None if self._files.stderr is None else (self._files.stderr.local_path if self.is_local else self._files.stderr.remote_path),
        )

    def observation(self):
        """Declare byte-delivery hooks without transferring file ownership."""
        return {
            "stream_offsets": self._observation_offsets(),
            "stream_writer": None if self.is_local else self._write_output,
            "stream_paths": self.stream_paths,
        }

    def _observation_offsets(self):
        if self.is_local:
            return {"out": 0, "err": 0}
        offsets = {}
        streams = self._streams
        for kind, target in (
            ("out", None if streams is None else streams.output_file),
            ("err", None if streams is None else streams.error_file),
        ):
            if target is None:
                offsets[kind] = None
            elif kind == "err" and target is streams.output_file:
                offsets[kind] = offsets["out"]
            else:
                target.flush()
                offsets[kind] = os.path.getsize(self._local_path(target))
        self._stream_positions = dict(offsets)
        return offsets

    async def _verify_streams(self):
        """Confirm that streamed files exactly match the authoritative target."""
        if self._files is None:
            return
        checked = set()
        for kind, entry in (("stdout", self._files.stdout), ("stderr", self._files.stderr)):
            if entry is None or entry.local_path is None or entry.remote_path is None:
                continue
            local = entry.local_path
            if local in checked:
                continue
            checked.add(local)
            try:
                signature = await self.context.transport.file_signature(entry.remote_path)
                if signature is None:
                    raise FileNotFoundError(f"Remote {kind} output is missing: {entry.remote_path}")
                stat = os.stat(local)
                if stat.st_size != signature[0]:
                    raise RuntimeError(f"Streamed {kind} size does not match remote output: {local}")
                digest = await self.context.transport.file_digest(entry.remote_path)
                if digest is None or file_digest(local) != digest:
                    raise RuntimeError(f"Streamed {kind} differs from remote output: {local}")
                if await self.context.transport.file_signature(entry.remote_path) != signature:
                    raise RuntimeError(f"Remote {kind} changed during verification")
                os.utime(local, ns=(stat.st_atime_ns, signature[1]))
            except Exception as exc:
                self._set_file_availability(local, "failed", str(exc))
                raise

    async def _retrieve_target_streams(self, *, required):
        # The target process wrote directly to named files. Release client
        # stream handles before downloading, so data is never truncated by a
        # stale local writer or blocked by an open descriptor on Windows.
        self.close()
        seen = set()
        for entry in (self._files.stdout, self._files.stderr):
            if entry is None or entry.local_path is None or entry.remote_path is None:
                continue
            if entry.local_path in seen:
                continue
            seen.add(entry.local_path)
            try:
                if await self.context.transport.file_signature(entry.remote_path) is None:
                    if required:
                        raise FileNotFoundError(f"Remote output stream is missing: {entry.remote_path}")
                    self._set_file_availability(entry.local_path, "skipped")
                    continue
                await self.context.transport.get_file(entry.remote_path, entry.local_path)
                self._set_file_availability(entry.local_path, "available")
            except Exception as exc:
                self._set_file_availability(entry.local_path, "failed", str(exc))
                raise

    async def _synchronize(self, state):
        if self.is_local:
            from kkrunner.files.availability import local_manifest
            from kkrunner.runs.base import RunStatus
            self.close()
            self._files = local_manifest(self._files, RunStatus(
                state, return_code=self._status.return_code,
                failure_phase=self._status.failure_phase, message=self._status.message))
            if state == RunState.FINISHED and self._status.return_code == 0 and self._files.failed_artifacts:
                raise FileNotFoundError(self._files.availability_message)
            return
        required = state == RunState.FINISHED and self._status.return_code == 0
        try:
            if self._retrieve_primary_streams:
                await self._retrieve_target_streams(required=required)
            await self._sync_output_files(required=required)
            await self._sync_cache_files()
            if required:
                await self._verify_streams()
        except Exception as exc:
            self._set_pending_failed(str(exc))
            raise
        if self._files is not None:
            for entry in self._files.artifacts:
                if entry.availability_state == "pending":
                    self._set_file_availability(entry.local_path, "available")

    def mark_transfer_failed(self, message):
        self._set_pending_failed(message)

    async def finalize_async(self, status):
        """Finalize owned files on the existing event loop, if any."""
        self._status = status
        if not status.state.is_terminal:
            raise RuntimeError("Cannot finalize files before terminal backend status")
        try:
            await self._synchronize(status.state)
        finally:
            self.close()
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
        else:
            # A supplied file object belongs to this session even if staging
            # failed before the runtime stream acquisition.
            OutputStreams(
                self.spec.output_file if hasattr(self.spec.output_file, "close") else None,
                self.spec.error_file if hasattr(self.spec.error_file, "close") else None,
            ).close()
