"""Client-side staging and terminal result-file synchronization.

This session never submits, observes, waits for, or cancels a backend job. A
Run owns the session and invokes it before and after a distinct Job.
"""

from __future__ import annotations

import glob
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
from kkrunner.files.paths import file_path, has_file_magic, path_matches
from kkrunner.files.queries import directory_digest, local_file_signature
from kkrunner.files.streams import OutputStreams
from kkrunner.files.versions import FileVersion, file_digest
from kkrunner.runs.base import RunState, SubmissionNotStarted
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
        self.input_file = self._local_path(spec.input_file)
        self.output_file = self._local_path(spec.output_file)
        self.error_file = self._local_path(spec.error_file)

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
        return not self.context.transfers_files

    def _local_path(self, value):
        if value is None:
            return None
        try:
            return file_path(value, self.spec.directory, required=not self.is_local)
        except ValueError as exc:
            raise ValueError("Remote execution requires named files") from exc

    async def _stage_file(self, entry, used, *, required=True, uploads=None, remote_values=None):
        if entry is None:
            return None
        source, target = entry.local_path, entry.remote_path
        previous = used.get(target)
        if previous is not None and previous != source:
            raise ValueError(f"Remote input target collision: {target}")
        if previous is not None:
            return target

        if not self.context.transfers_files and not os.path.isfile(source):
            if required:
                raise FileNotFoundError(f"Shared input file does not exist: {source}")
            return None
        policy = self.context.input_files
        if policy == FileSyncPolicy.NEVER:
            used[target] = source
            return target

        # Only CHANGED needs content identity to decide whether to upload.
        # ALWAYS with a local source does not need to inspect the target at all.
        # The authoritative input digest is captured after staging.
        compare = policy == FileSyncPolicy.CHANGED
        local_value = (file_digest(source, missing_ok=True) if compare
                       else local_file_signature(source))
        if policy == FileSyncPolicy.ALWAYS and local_value is None and required:
            raise FileNotFoundError(f"Local input file does not exist: {source}")
        if policy == FileSyncPolicy.ALWAYS and local_value is not None:
            remote_value = None
        elif remote_values is not None:
            remote_value = remote_values.get(target)
        elif compare:
            remote_value = await self.context.target.file_digest(target)
        else:
            remote_value = await self.context.target.file_signature(target)

        if local_value is None and remote_value is None:
            if required:
                raise FileNotFoundError(f"Input file exists neither locally nor remotely: {source}")
            return None

        if policy.should_transfer(local_value, remote_value):
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
        if not self.context.transfers_files and not os.path.isdir(source):
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
        if policy == FileSyncPolicy.ALWAYS and local_exists:
            # Publication validates the destination; no preliminary query
            # can change an unconditional upload decision.
            remote_exists = False
        else:
            if required and policy == FileSyncPolicy.ALWAYS:
                raise FileNotFoundError(f"Local input directory does not exist: {source}")
            observed = (await transport.query_files([target]))[0]
            remote_exists = observed["status"] == "ok" and observed["type"] == "directory"
            if observed["status"] == "ok" and not remote_exists:
                raise ValueError(f"Target directory input is not a directory: {target}")
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

    async def _stage_input_pattern(self, matches, used, remote_root, *, required=True):
        """Stage a concrete, previously resolved local input set."""
        if not matches:
            return
        policy = self.context.input_files
        pending = [match.remote_path for match in matches if match.remote_path not in used]
        values = None
        if pending and policy in (FileSyncPolicy.CHANGED, FileSyncPolicy.MISSING):
            compare = policy == FileSyncPolicy.CHANGED
            observed = await self.context.target.query_files(pending, digest=compare)
            values = {
                item["path"]: item.get("digest") if compare else True
                for item in observed if item["status"] == "ok" and item["type"] == "file"
            }
        uploads = []
        for match in matches:
            await self._stage_file(match, used, required=required, uploads=uploads,
                                   remote_values=values)
        if uploads:
            await self.context.target.put_files(
                uploads, source_directory=self.context.local_root,
                destination_directory=remote_root,
            )

    async def _prepare_cache_files(self, used, manifest, input_patterns):
        exact = []
        patterns = []
        remote_root = manifest.mapping.remote_root

        for cache in manifest.caches:
            entry = cache.file
            if has_file_magic(entry.local_path):
                patterns.append(entry.remote_path)
                await self._stage_input_pattern(
                    input_patterns[entry.local_path], used, remote_root, required=False,
                )
            else:
                if entry.kind == "directory":
                    await self._stage_directory(entry, used, required=False)
                else:
                    await self._stage_file(entry, used, required=False)
                # The declaration remains even when no existing cache was staged.
                exact.append(entry.remote_path)

        signatures = {}
        if self.context.output_files != FileSyncPolicy.NEVER and (patterns or exact):
            # Capture pre-run baselines after staging in one metadata request.
            entries = await self.context.target.query_files(
                [*patterns, *exact], root=remote_root,
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

        exact = set(exact)
        return (
            *(replace(cache, before=signatures.get(cache.file.remote_path))
              if cache.file.remote_path in exact else cache for cache in manifest.caches),
            *(CacheEntry(FileEntry(remote_path=remote), before)
              for remote, before in signatures.items() if remote not in exact),
        )

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

    def _pattern_declarations(self, kind):
        entries = (
            (cache.file for cache in self._files.caches) if kind == "cache"
            else self._files.outputs
        )
        return tuple(entry for entry in entries if entry.collect
                     and entry.local_path is not None and entry.remote_path is not None
                     and entry.source_pattern is None and has_file_magic(entry.remote_path))

    async def _resolve_result_patterns(self, required, on_resolved):
        """Freeze all remote output/cache glob members before any download."""
        state = self._files.remote_patterns_resolved
        if state is False:
            raise OSError(self._files.availability_message or "Remote pattern discovery failed")
        if state is True:
            return
        patterns = [(kind, entry) for kind in ("output", "cache")
                    for entry in self._pattern_declarations(kind)]
        if not patterns:
            return
        manifest = self._files
        try:
            root = (manifest.mapping.remote_root if manifest.mapping is not None
                    else await self.context._resolved_remote_root())
            for kind, declaration in patterns:
                pattern = declaration.remote_path
                observed = await self.context.target.query_files([pattern], root=root)
                matches = [item["path"] for item in observed
                           if item["status"] == "ok" and item["type"] == "file"]
                if not matches and kind == "output" and required and not declaration.optional:
                    raise FileNotFoundError(f"Declared remote output pattern did not match any files: {pattern}")
                for remote in matches:
                    if kind == "output" and manifest.output_excluded(remote, remote=True):
                        continue
                    local = os.fspath(self.context._unmap_path(remote, root))
                    if any(entry.local_path == local for entry in manifest.artifacts):
                        continue
                    manifest = manifest.with_file_availability(
                        local, "pending", remote_path=remote, kind=kind,
                        source_pattern=declaration.local_path,
                    )
        except Exception as exc:
            # Do not publish a partial expansion or perform any transfers.
            self._files = replace(self._files.fail_pending_artifacts(str(exc)),
                                  remote_patterns_resolved=False)
            if on_resolved is not None:
                on_resolved(self._files)
            raise
        self._files = replace(manifest, remote_patterns_resolved=True)
        if on_resolved is not None:
            on_resolved(self._files)

    async def _sync_patterns(self, declarations, *, kind, required=False, seen_remote):
        """Transfer only previously discovered concrete members; never expand."""
        mapping = self._files.mapping
        remote_root = mapping.remote_root if mapping is not None else await self.context._resolved_remote_root()
        cache_before = ({cache.file.remote_path: cache.before for cache in self._files.caches
                         if cache.file.local_path is None} if kind == "cache" else {})
        entries = (
            tuple(cache.file for cache in self._files.caches) if kind == "cache"
            else (self._files.stdout, self._files.stderr, *self._files.outputs)
        )
        concrete = tuple(entry for entry in entries
                         if entry is not None and entry.collect and entry.kind == "file"
                         and entry.local_path is not None and entry.remote_path is not None
                         and (entry.source_pattern is not None or not has_file_magic(entry.remote_path)))
        transport = self.context.target
        policy = self.context.output_files
        compare = kind == "output" or policy == FileSyncPolicy.NEVER
        for declaration in declarations:
            pattern = declaration.remote_path
            local_pattern = declaration.local_path
            mandatory = required and not declaration.optional
            members = tuple(entry for entry in concrete if path_matches(entry.remote_path, pattern)
                            and (kind != "output" or not self._files.output_excluded(entry.remote_path, remote=True)))
            pending = []
            try:
                candidates = []
                for entry in members:
                    remote = entry.remote_path
                    if remote in seen_remote:
                        continue
                    seen_remote.add(remote)
                    if self._published_locally(entry) or entry.availability_state == "skipped":
                        continue
                    if policy == FileSyncPolicy.NEVER and not os.path.exists(entry.local_path):
                        self._set_file_availability(entry.local_path, "skipped", kind=kind)
                        continue
                    self._set_file_availability(entry.local_path, "pending", kind=kind)
                    candidates.append(entry)
                # No root argument: query_files treats these names as literals.
                observed = await transport.query_files(
                    [entry.remote_path for entry in candidates],
                    digest=compare and policy != FileSyncPolicy.ALWAYS,
                ) if candidates else ()
                by_path = {item["path"]: item for item in observed}
                downloads = []
                for entry in candidates:
                    local, remote = entry.local_path, entry.remote_path
                    item = by_path[remote]
                    if compare:
                        available, transfer = self._output_file_action(local, remote, mandatory, item)
                    else:
                        signature = ((item["size"], item["mtime_ns"])
                                     if item["status"] == "ok" and item["type"] == "file" else None)
                        available = transfer = self._cache_file_changed(
                            local, cache_before.get(remote), signature,
                        )
                    if transfer:
                        downloads.append((remote, local))
                        pending.append(local)
                    else:
                        self._set_file_availability(local, "available" if available else "skipped", kind=kind)
                if downloads:
                    await transport.get_files(downloads, source_directory=remote_root,
                                              destination_directory=self.context.local_root)
                for local in pending:
                    self._set_file_availability(local, "available", kind=kind)
                matched_paths = {entry.local_path for entry in members}
                available = any(entry.local_path in matched_paths and entry.availability_state == "available"
                                for entry in self._files.artifacts)
                self._set_file_availability(local_pattern, "available" if available else "skipped", kind=kind)
            except Exception as exc:
                self._set_file_availability(local_pattern, "failed", str(exc), kind=kind)
                raise

    @staticmethod
    def _published_locally(entry):
        """A previous collection already published this concrete artifact."""
        return entry.availability_state == "available" and (
            os.path.isdir(entry.local_path) if entry.kind == "directory"
            else os.path.isfile(entry.local_path)
        )

    async def _sync_output_files(self, required):
        # Primary streams are ordinary output files, not a reconstruction
        # of the live FOLLOW stream. NEVER disables transfers for all outputs;
        # existing identical copies (including shared mounts) remain usable.
        required = required and self.context.output_files != FileSyncPolicy.NEVER
        declarations = tuple(entry for entry in (self._files.stdout, self._files.stderr, *self._files.outputs)
                             if entry is not None and entry.collect and entry.source_pattern is None
                             and entry.remote_path is not None and entry.local_path is not None)
        seen_remote = set()
        for entry in declarations:
            if has_file_magic(entry.remote_path) or entry.remote_path in seen_remote:
                continue
            seen_remote.add(entry.remote_path)
            # A completed transfer belongs to this run even if its remote
            # source is no longer available on a later collection attempt.
            if self._published_locally(entry):
                continue
            await self._sync_output_entry(
                entry.local_path, entry.remote_path, required and not entry.optional,
                kind=entry.kind,
            )

        patterns = [entry for entry in declarations if has_file_magic(entry.remote_path)]
        if patterns:
            await self._sync_patterns(
                patterns, kind="output", required=required, seen_remote=seen_remote,
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
        entry = (await self.context.target.query_files(
            [remote], digest=policy != FileSyncPolicy.ALWAYS,
        ))[0]
        available, transfer = self._output_file_action(local, remote, required, entry)
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

    def _output_file_action(self, local, remote, required, entry):
        """Decide availability and transfer using the same exact/pattern policy."""
        if entry["status"] != "ok" or entry["type"] != "file":
            if required:
                raise FileNotFoundError(f"Declared remote output file was not created: {remote}")
            return False, False
        policy = self.context.output_files
        if policy == FileSyncPolicy.ALWAYS:
            return True, True
        remote_digest = entry["digest"]
        local_digest = file_digest(local, missing_ok=True)
        if policy.should_transfer(remote_digest, local_digest):
            return True, True
        # MISSING can preserve a stale copy which must not become an artifact.
        return local_digest is not None and local_digest == remote_digest, False

    def _cache_file_changed(self, local, before, after):
        return after is not None and after != before and not (
            self.context.output_files == FileSyncPolicy.MISSING
            and local_file_signature(local) is not None
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
        exact = set()
        patterns = []
        for cache in self._files.caches:
            entry = cache.file
            if entry.remote_path is None or entry.local_path is None or entry.source_pattern is not None:
                continue
            if has_file_magic(entry.remote_path):
                patterns.append(entry)
            else:
                exact.add(entry.remote_path)
                if self._published_locally(entry):
                    continue
                await self._sync_cache_entry(
                    entry.local_path, entry.remote_path, cache.before, kind=entry.kind,
                )

        if patterns:
            await self._sync_patterns(patterns, kind="cache", seen_remote=exact)

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
        from kkrunner.support.durability import sync_file

        used = {}
        primary = next((entry for entry in manifest.inputs
                        if entry.local_path == self.input_file), None)
        # Resolve all local source patterns before the first upload. The
        # staging routines below consume only these exact source/target pairs.
        input_patterns = {}
        for entry in (*manifest.inputs, *(cache.file for cache in manifest.caches)):
            if entry is primary or not has_file_magic(entry.local_path):
                continue
            if entry.local_path not in input_patterns:
                input_patterns[entry.local_path] = self.context.expand_input(
                    entry, manifest.mapping.remote_root,
                )
            if entry in manifest.inputs and not entry.optional and not input_patterns[entry.local_path]:
                raise FileNotFoundError(f"Local input pattern did not match any files: {entry.local_path}")

        local_inputs = dict.fromkeys(
            path for entry in manifest.input_entries
            for path in (
                tuple(match.local_path for match in input_patterns[entry.local_path])
                if entry.local_path in input_patterns else (entry.local_path,)
            )
        )
        for path in local_inputs:
            if os.path.isfile(path):
                sync_file(path)
        input_target = await self._stage_file(primary, used)
        stdin_target = await self._stage_file(manifest.stdin, used)
        for entry in manifest.inputs:
            if entry is primary:
                continue
            required = not entry.optional
            if has_file_magic(entry.local_path):
                await self._stage_input_pattern(
                    input_patterns[entry.local_path], used, manifest.mapping.remote_root,
                    required=required,
                )
            elif entry.kind == "directory":
                await self._stage_directory(entry, used, required=required)
            else:
                await self._stage_file(entry, used, required=required)

        inputs = dict(used)
        caches = await self._prepare_cache_files(used, manifest, input_patterns)
        staged = {source: target for target, source in used.items()}

        output_target = None if manifest.stdout is None else manifest.stdout.remote_path
        error_target = None if manifest.stderr is None else manifest.stderr.remote_path

        request = target_request(
            self.spec, remote_directory,
            transform=lambda args: self._remote_argv(args, staged),
            input_file=input_target, stdin=stdin_target,
            output_file=output_target, error_file=error_target,
            output_files=(entry.remote_path for entry in manifest.outputs),
            output_directories=(entry.remote_path for entry in manifest.outputs if entry.kind == "directory"),
            cache_files=(cache.file.remote_path for cache in manifest.caches),
            # Target cleanup consumes glob declarations. These staged paths
            # are already concrete and must not be interpreted a second time.
            input_files=(glob.escape(target) for target in used),
        )
        versions = await self._input_versions(manifest, inputs)
        return request, replace(manifest, caches=caches, input_versions=versions)

    async def _target_versions(self, paths):
        transport = self.context.target
        files = [target for target, (_, kind) in paths.items() if kind == "file"]
        observed = await transport.query_files(files, digest=True) if files else ()
        digests = {item["path"]: item["digest"] for item in observed if "digest" in item}
        versions = []
        for target, (local, kind) in paths.items():
            digest = digests.get(target)
            if kind == "directory":
                digest = await transport.call(Operation.standalone(directory_digest), target)
            if digest is not None:
                versions.append(FileVersion(local, fingerprint=None, digest=digest, kind=kind))
        return tuple(versions)

    async def _input_versions(self, manifest, inputs):
        directories = {entry.local_path for entry in manifest.input_entries if entry.kind == "directory"}
        paths = {
            target: (local, "directory" if local in directories else "file")
            for target, local in inputs.items()
        }
        if self.context.transfers_files:
            return await self._target_versions(paths)
        # Shared input staging already selected and validated exact members.
        # Do not expand the original declarations again after synchronization.
        return tuple(FileVersion.capture(local, kind=kind, require_digest=True)
                     for local, kind in sorted(paths.values()))

    async def _target_cache_versions(self):
        return await self._target_versions({
            entry.remote_path: (entry.local_path, entry.kind)
            for cache in self._files.caches
            if (entry := cache.file).remote_path is not None and entry.local_path is not None
            and (entry.source_pattern is not None or not has_file_magic(entry.remote_path))
        })

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
        from kkrunner.files.availability import snapshot_file_entries

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
                versions = snapshot_file_entries(manifest.input_entries, sync=True)
                return request, replace(manifest, input_versions=versions)
            except OSError as exc:
                raise SubmissionNotStarted(str(exc)) from exc

        try:
            directory, manifest = await self._resolve_footprint()
            if (manifest.stdout is not None and manifest.stderr is not None
                    and manifest.stdout.local_path == manifest.stderr.local_path):
                raise ValueError("Remote stdout and stderr must have distinct output files")
            self._flush_declared_handles()
            await self.context.target.run(("mkdir", "-p", "--", directory.target))
            return await self._prepare_request(directory.target, manifest)
        except Exception as exc:
            # No native calculation has been launched.
            raise SubmissionNotStarted(str(exc)) from exc

    @classmethod
    def from_attachment(cls, target, attachment):
        """Restore file declarations and their mapping without target I/O."""
        from kkrunner.client.file_context import FileContext
        from kkrunner.client.staging import StagingSpec
        from kkrunner.runs.base import ExecutionSpec

        execution, files = attachment.execution, attachment.files
        if not target.is_local and execution.directory.remote is None:
            raise ValueError("Remote run lacks working directory")
        spec = ExecutionSpec(
            argv=None, directory=execution.directory.local, input_file=files.input_file,
            output_file=files.output_file, error_file=None if files.stderr is None else files.stderr.local_path,
            staging=StagingSpec.from_mapping(files.mapping),
        )
        context = FileContext(target, spec.staging)
        return cls(context, spec, files=files, remote_directory=execution.directory.target)

    @property
    def working_directory(self):
        from kkrunner.runs.model import WorkingDirectory
        return WorkingDirectory(os.path.abspath(self.spec.directory),
                                None if self.is_local else self._remote_directory)

    async def _resolve_footprint(self):
        if self._remote_directory is None:
            if self.is_local:
                self._remote_directory = os.path.abspath(self.spec.directory)
            else:
                manifest = await self.context.map_manifest(self._files)
                self._remote_directory = self.context._map_path(
                    self.spec.directory, manifest.mapping.remote_root,
                )
                self._files = manifest
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
        await self._sync_output_files(required=required)
        await self._sync_cache_files()
        # A previously discovered wildcard child may have disappeared
        # before a retry. Only a synchronization decision can publish it.
        self._files = self._files.skip_pending_artifacts()

    async def finalize_async(self, status, *, on_resolved=None):
        """Finalize owned files on the existing event loop, if any."""
        if not status.state.is_terminal:
            raise RuntimeError("Cannot finalize files before terminal backend status")
        from kkrunner.runs.states import NO_EXECUTION_FAILURES

        try:
            self.close()
            if self._files.captured_at is not None:
                # Finalization is not historical restoration. Never modify
                # files whose provenance has already been captured.
                return self._files
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
                await self._resolve_result_patterns(
                    required and self.context.output_files != FileSyncPolicy.NEVER, on_resolved,
                )
                await self._synchronize(required=required)
            from kkrunner.files.availability import capture_result_files

            self._files = capture_result_files(
                self._files, require_outputs=required,
                cache_versions=None if self.files_are_local else await self._target_cache_versions(),
            )
            return self._files
        except Exception as exc:
            self._files = self._files.fail_pending_artifacts(str(exc))
            raise

    def finalize(self, status, *, on_resolved=None):
        """Synchronously finalize using the same file authority as SSH recovery."""
        return _run_sync(self.finalize_async(status, on_resolved=on_resolved))

    def close(self):
        """Release owned streams once, even if cleanup itself raises."""
        if self._closed:
            return
        self._closed = True
        streams, self._streams = self._streams, None
        if streams is not None:
            streams.close()

