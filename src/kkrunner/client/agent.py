"""Remote execution with client-owned lifecycle persistence."""

from __future__ import annotations

import glob
import os
import posixpath
import warnings
from dataclasses import replace
from pathlib import Path, PurePosixPath

from kkrunner.client.remote_paths import RemotePathContext, _relative_local_path, query_remote_home
from kkrunner.client.endpoint import endpoint_config_for_executor
from kkrunner.files.manifest import FileMapping, FileSyncPolicy
from kkrunner.remote.protocol import control_command, json_request
from kkrunner.client.remote_files import RemoteFileSession
from kkrunner.client.job import Job
from kkrunner.client.run_parts import RunParts
from kkrunner.client.contracts import ReopenableRunEndpoint
from kkrunner.support.async_utils import run_coro_sync as _run_sync
from kkrunner.remote.stream import FramedRunningCommand
from kkrunner.runs.base import (
    FailurePhase,
    LaunchSpec,
    MpiNotFoundWarning,
    RunState,
    RunStatus,
)
from kkrunner.runs.model import WorkingDirectory

from kkrunner.backends.registry import installed_backend_names


def _remote_status_from_facts(facts, backend_id, *, execution=None, files=None):
    """Interpret one backend reply without issuing another remote request."""
    return RunStatus(
        RunState(facts.get("state", "unknown")),
        backend_id=backend_id,
        execution=execution,
        files=files,
        return_code=facts.get("return_code"),
        message=facts.get("error"),
        failure_phase=facts.get("failure_phase"),
    )


class _AgentRunContext(RemotePathContext):
    @property
    def backend(self):
        """Execution backend identity is owned by the endpoint."""
        return self.endpoint.backend

    async def _submit(self, request):
        return await self.endpoint._submit(request)

    async def _status(self, directory, backend_id, files=None):
        return await self.endpoint._status(directory, backend_id, files)

    async def _cancel(self, directory, backend_id, files=None):
        return await self.endpoint._cancel(directory, backend_id, files)

    async def _follow(
        self, directory, backend_id, *, offsets=None,
        stdout_path=None, stderr_path=None,
    ):
        return await self.endpoint._follow(
            directory, backend_id, offsets=offsets,
            stdout_path=stdout_path, stderr_path=stderr_path,
        )

    def create_run_parts(self, spec, *, submission_key=None, callback=None):
        files = RemoteFileSession(self, spec)
        job = Job(self, spec, callback=callback)
        return RunParts(
            job, files,
            submission_builder=lambda prepared: self.endpoint.submission_request(
                prepared, submission_key=submission_key,
            ),
        )

    async def attach_run_parts(self, attachment, *, callback=None, output_listener=None):
        execution = attachment.execution
        if execution.backend_id is None or execution.directory.remote is None:
            raise ValueError("Remote run lacks backend identity or target directory")
        status = await self._status(execution.directory.remote, execution.backend_id)
        files = await RemoteFileSession.from_attachment(self, attachment)
        try:
            job = Job(
                self, files.spec, callback=callback,
                output_listener=output_listener,
                backend_id=execution.backend_id,
                remote_directory=execution.directory.remote,
                status=status,
            )
            return RunParts(job, files)
        except BaseException:
            files.close()
            raise


class AgentRunEndpoint(ReopenableRunEndpoint):
    default_persistent = True
    default_detached = True

    """Reusable remote endpoint with client-owned lifecycle state."""

    name = "agent"
    supports_result_file_retrieval = True

    def __init__(
        self,
        transport,
        *,
        backend="process",
        backend_options=None,
        command=None,
        filesystem_id=None,
    ):
        if backend not in installed_backend_names():
            raise ValueError(f"Unsupported remote executor: {backend!r}")
        self.transport = transport
        self.filesystem_id = filesystem_id
        self.backend = backend
        self.backend_options = dict(backend_options or {})
        self.command = tuple(command if command is not None else getattr(transport, "command", ("kkrunner-agent",)))
        if hasattr(transport, "command"):
            transport.command = self.command
        self.computer = getattr(transport, "host", None)
        self._remote_home_cache = None

    async def _remote_home(self):
        if self._remote_home_cache is None:
            self._remote_home_cache = await query_remote_home(self.transport)
        return self._remote_home_cache

    def submission_request(self, prepared_files, *, submission_key=None):
        """Only the execution endpoint chooses backend protocol and durability."""
        return {
            **prepared_files,
            "id": submission_key,
            "executor": self.backend,
            "executor_options": dict(self.backend_options),
            "detached": True,
        }

    async def _json_request(self, action, *args, payload=None):
        return await json_request(self.transport, self.command, action, *args, payload=payload)

    def _control_command(self, action, *args):
        return control_command(self.command, action, *args)

    @staticmethod
    def _context(endpoint, launch):
        if not isinstance(launch, LaunchSpec):
            launch = LaunchSpec(**dict(launch))
        return _AgentRunContext(endpoint, launch)

    def _validate_run(self, run):
        if run.execution.endpoint != endpoint_config_for_executor(self):
            raise ValueError("Run belongs to a different remote endpoint")
        if not run.execution.backend_id or not run.execution.directory.remote:
            raise ValueError("Remote run lacks backend identity or working directory")

    @staticmethod
    def _launch_from_manifest(files):
        mapping = files.mapping
        if mapping is None:
            raise ValueError("Remote run has no persisted file mapping")
        return LaunchSpec(
            local_root=mapping.local_root,
            remote_root=mapping.remote_root,
            input_sync=mapping.input_sync,
            output_sync=mapping.output_sync,
        )

    async def _resolve_submission_directory(self, directory):
        directory = posixpath.normpath(os.fspath(directory))
        if directory == "~":
            return await self._remote_home()
        if directory.startswith("~/"):
            return posixpath.join(await self._remote_home(), directory[2:])
        return directory

    def submission_directory(self, spec):
        launch = spec.launch
        relative = _relative_local_path(
            spec.directory,
            launch.local_root or Path.home(),
        )

        root = launch.remote_root
        if root is None or root == "~":
            remote_root = "~"
        elif root.startswith("~/"):
            remote_root = posixpath.normpath(root)
        elif posixpath.isabs(root):
            remote_root = posixpath.normpath(root)
        else:
            remote_root = posixpath.normpath(posixpath.join("~", root))
        if not relative.parts:
            return remote_root
        return posixpath.join(remote_root, *PurePosixPath(*relative.parts).parts)

    def resolve_submission(self, record, spec):
        """Resolve the footprint without uploads, mkdir, or opening output files."""

        async def resolve():
            context = self._context(self, spec.launch)
            directory = await context.map_directory(spec.directory)

            async def mapped(entry):
                if entry is None or entry.local_path is None:
                    return entry
                return replace(entry, remote_path=await context.map_path(entry.local_path))

            files = record.files
            files = replace(
                files,
                inputs=tuple([await mapped(entry) for entry in files.inputs]),
                outputs=tuple([await mapped(entry) for entry in files.outputs]),
                caches=tuple([replace(cache, file=await mapped(cache.file)) for cache in files.caches]),
                stdin=await mapped(files.stdin),
                stdout=await mapped(files.stdout),
                stderr=await mapped(files.stderr),
                mapping=FileMapping(
                    local_root=str(context.local_root),
                    remote_root=await context._resolved_remote_root(),
                    input_sync=context.input_files.value,
                    output_sync=context.output_files.value,
                ),
            )
            return replace(
                record,
                files=files,
                execution=replace(
                    record.execution,
                    directory=WorkingDirectory(record.execution.directory.local, directory),
                ),
            )

        return _run_sync(resolve())

    async def _submit(self, request):
        launch = request.get("launch") or {}
        mpi = launch.get("mpi") or {}
        if self.backend == "process" and mpi.get("autodetect") and not mpi.get("required") and mpi.get("warning", True):
            resolved = await self._json_request("resolve-mpi", payload={"mpi": "auto"})
            if not resolved.get("mpi"):
                warnings.warn(
                    "MPI autodetection did not find an MPI launcher on the remote "
                    "host; the calculation will run without MPI.",
                    MpiNotFoundWarning,
                    stacklevel=3,
                )
        return await self._json_request("submit", request["directory"], payload=request)

    async def _status(self, directory, backend_id, files=None):
        return await self._json_request("status", backend_id)

    async def _cancel(self, directory, backend_id, files=None):
        return await self._json_request("cancel", backend_id)

    async def _follow(
        self, directory, backend_id, *, offsets=None,
        stdout_path=None, stderr_path=None,
    ):
        options = []
        if stdout_path:
            options.extend(("--stdout-path", stdout_path))
        if stderr_path:
            options.extend(("--stderr-path", stderr_path))
        for kind, option in (("out", "--stdout-offset"), ("err", "--stderr-offset")):
            offset = None if offsets is None else offsets.get(kind)
            if offset is not None:
                options.extend((option, str(offset)))
            elif offsets is not None:
                options.append("--live-" + ("stdout" if kind == "out" else "stderr"))
        process = await self.transport.start(self._control_command("follow", str(backend_id), *options, "--framed"))
        return FramedRunningCommand(process)

    def acknowledge_submission(self, run):
        """Release temporary submission identity after client persistence."""
        key = run.execution.submission_key or run.id
        return _run_sync(self._json_request("ack-submit", self.backend, key))

    def acknowledge(self, run):
        """Release a delivered receipt only after durable RunRecord completion."""
        self._validate_run(run)
        return _run_sync(self._json_request("ack", run.execution.backend_id))

    def status(self, *runs):
        for run in runs:
            self._validate_run(run)
        if not runs:
            return ()

        async def query():
            result = []
            for run in runs:
                execution = run.execution
                state = await self._status(execution.directory.remote, execution.backend_id, run.files)
                result.append(
                    _remote_status_from_facts(
                        state,
                        execution.backend_id,
                    )
                )
            return tuple(result)

        return _run_sync(query())

    async def _recover_submission(self, run_id, submission, *, submitted_at=None):
        directory = await self._resolve_submission_directory(submission.execution.directory.target)
        state = await self._json_request(
            "recover",
            directory,
            run_id,
            payload={
                "executor": self.backend,
                "executor_options": self.backend_options,
                "submitted_at": submitted_at,
            },
        )
        from kkrunner.backends.contracts import JobRef, RecoveryResult
        if state is None:
            return RecoveryResult.indeterminate("Remote submission receipt was not found")
        if state.get("unsupported"):
            return RecoveryResult.unsupported(state.get("error"))
        remote_id = state.get("remote_id")
        if not remote_id:
            raise RuntimeError("Recovered submission has no remote_id")
        return RecoveryResult.found(
            JobRef(self.backend, str(remote_id)),
            directory=state.get("directory") or directory,
            output_file=state.get("output_file"),
            error_file=state.get("error_file"),
        )

    def recover_submission(self, run_id, submission, *, submitted_at=None):
        return _run_sync(self._recover_submission(run_id, submission, submitted_at=submitted_at))

    def create_run_parts(self, spec, *, submission_key=None, callback=None):
        from uuid import uuid4
        spec.require_complete()
        return self._context(self, spec.launch).create_run_parts(
            spec, submission_key=submission_key or uuid4().hex, callback=callback,
        )

    async def _snapshot_file(self, run, file, *, destination=None):
        """Observe target-side content without affecting normal result downloads."""
        self._validate_run(run)
        context = self._context(self, self._launch_from_manifest(run.files))
        _relative_local_path(file, context.local_root)
        entry = next(
            (item for item in (
                run.files.stdout, run.files.stderr, *run.files.outputs,
                *(cache.file for cache in run.files.caches),
            ) if item is not None and item.local_path == file),
            None,
        )
        remote = entry.remote_path if entry is not None else None
        if remote is None:
            remote = await context.map_path(file)
        root = await context._resolved_remote_root()
        normalized = posixpath.normpath(remote)
        if not posixpath.isabs(normalized) or posixpath.commonpath((root, normalized)) != root:
            raise ValueError(f"Remote snapshot path is outside mapped root: {remote}")
        # Also reject remote symlinks that escape the mapped filesystem root.
        # query_files checks real path containment without requiring a stable file.
        observed = await self.transport.query_files([normalized], root=root)
        if not observed or observed[0]["status"] == "missing":
            raise FileNotFoundError(normalized)
        if observed[0]["type"] != "file":
            raise ValueError(f"Snapshot source is not a regular file: {normalized}")
        return await self.transport.snapshot_file(normalized, destination=destination)

    def snapshot_file(self, run, file, *, destination=None):
        return _run_sync(self._snapshot_file(run, file, destination=destination))

    async def _fetch_result_files(self, run, files, *, destinations=None):
        self._validate_run(run)
        context = self._context(self, self._launch_from_manifest(run.files))
        remote_root = await context._resolved_remote_root()
        destinations = (
            {}
            if destinations is None
            else {os.path.abspath(os.fspath(path)): os.fspath(target) for path, target in destinations.items()}
        )
        for value in files:
            local = os.path.abspath(os.fspath(value))
            entry = next(
                (
                    item
                    for item in (
                        *run.files.inputs,
                        *run.files.outputs,
                        *(cache.file for cache in run.files.caches),
                        run.files.stdout,
                        run.files.stderr,
                    )
                    if item is not None and item.local_path == local
                ),
                None,
            )
            remote = entry.remote_path if entry is not None else None
            if remote is None:
                remote = await context.map_path(local)
            # Exact paths and wildcards have the same source-side selection
            # and use the transport's verified single-file download.
            for matched in await self.transport.expand_files(remote, remote_root):
                destination = os.fspath(await context.unmap_path(matched)) if glob.has_magic(remote) else local
                _relative_local_path(destination, context.local_root)
                target = destinations.get(destination, destination)
                if not os.path.isfile(target):
                    await self.transport.get_file(matched, target)

    def fetch_result_files(self, run, files, *, destinations=None):
        return _run_sync(
            self._fetch_result_files(
                run,
                files,
                destinations=destinations,
            )
        )

    async def _open_run_parts(self, attachment, *, callback=None, output_listener=None):
        self._validate_run(attachment)
        launch = self._launch_from_manifest(attachment.files)
        return await self._context(self, launch).attach_run_parts(
            attachment, callback=callback, output_listener=output_listener,
        )

    def open_run_parts(self, attachment, *, callback=None, output_listener=None):
        return _run_sync(
            self._open_run_parts(
                attachment, callback=callback, output_listener=output_listener,
            )
        )
