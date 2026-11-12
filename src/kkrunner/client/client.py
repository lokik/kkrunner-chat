"""One execution client; transport selects direct calls or SSH RPC."""

from __future__ import annotations

import os
import posixpath
from dataclasses import replace
from pathlib import Path, PurePosixPath

from kkrunner.backends import configured_backend
from kkrunner.backends.config import BackendConfig
from kkrunner.backends.contracts import BackendCapability
from kkrunner.client.contracts import RunClientProtocol
from kkrunner.client.file_context import (
    FileContext,
    _relative_local_path,
    query_remote_home,
)
from kkrunner.client.file_session import FileSession
from kkrunner.client.job import Job
from kkrunner.client.operations import BackendOperation
from kkrunner.client.run_parts import RunParts
from kkrunner.client.staging import StagingSpec
from kkrunner.files.manifest import FileMapping
from kkrunner.runs.base import (
    RunState,
    RunStatus,
)
from kkrunner.runs.model import WorkingDirectory
from kkrunner.support.async_utils import run_coro_sync as _run_sync


def _run_status_from_facts(facts, backend_id, *, execution=None, files=None):
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


class _RunContext(FileContext):
    @property
    def backend(self):
        """Backend selection belongs to the configured client."""
        return self.client.backend

    async def _submit(self, request):
        return await self.client._submit(request)

    async def _status(self, directory, backend_id, files=None):
        return await self.client._status(directory, backend_id, files)

    async def _cancel(self, directory, backend_id, files=None):
        return await self.client._cancel(directory, backend_id, files)

    async def _follow(
        self, directory, backend_id, *, on_output, offsets=None,
        stdout_path=None, stderr_path=None,
    ):
        return await self.client._follow(
            directory, backend_id, on_output=on_output, offsets=offsets,
            stdout_path=stdout_path, stderr_path=stderr_path,
        )

    def create_run_parts(self, spec, *, submission_key=None, callback=None):
        files = FileSession(self, spec)
        if self.backend.execution_mode == "direct":
            job = Job.direct(spec, callback=callback, adapter=self.transport.direct_channel(self, files))
        else:
            job = Job(self, spec, callback=callback)
        return RunParts(
            job, files,
            submission_builder=lambda prepared: self.client.submission_request(
                prepared, submission_key=submission_key,
            ),
        )

    async def attach_run_parts(self, attachment, *, callback=None, output_listener=None):
        execution = attachment.execution
        if execution.backend_id is None:
            raise ValueError("Remote run lacks backend identity or target directory")
        status = await self._status(execution.directory.target, execution.backend_id)
        files = await FileSession.from_attachment(self, attachment)
        try:
            job = Job(
                self, files.spec, callback=callback,
                output_listener=output_listener,
                backend_id=execution.backend_id,
                remote_directory=execution.directory.target,
                status=status,
            )
            return RunParts(job, files)
        except BaseException:
            files.close()
            raise


class RunClient(RunClientProtocol):
    """Compose an execution backend and transport with client lifecycle state."""

    supports_result_file_retrieval = True

    def __init__(
        self,
        backend=None,
        *,
        transport=None,
        backend_options=None,
    ):
        backend = configured_backend(backend, options=backend_options)
        from kkrunner.client.transports import LocalTransport
        self.transport = LocalTransport() if transport is None else transport
        self.backend = backend
        self.computer = getattr(self.transport, "host", None)
        self._remote_home_cache = None

    @classmethod
    def from_config(cls, transport, backend):
        """Reconstruct a client from supported stored connection/backend data."""
        from kkrunner.client.transports import (
            require_persistable_transport,
            transport_from_dict,
        )
        if isinstance(transport, dict):
            transport = transport_from_dict(transport)
        if isinstance(backend, dict):
            backend = BackendConfig(**backend)
        require_persistable_transport(transport)
        return cls(backend.create(), transport=transport)

    @property
    def backend_config(self):
        return BackendConfig(self.backend.name, self.backend.options)

    @property
    def default_persistent(self):
        return self.backend.execution_mode == "managed"

    @property
    def default_detached(self):
        return self.backend.default_detached

    def _require_managed(self):
        if BackendCapability.REATTACH not in self.backend.supports:
            from kkrunner.backends.contracts import UnsupportedCapability
            raise UnsupportedCapability("Direct execution has no durable identity or reattachment")

    async def _remote_home(self):
        if self._remote_home_cache is None:
            self._remote_home_cache = await query_remote_home(self.transport)
        return self._remote_home_cache

    def submission_request(self, prepared_files, *, submission_key=None):
        """Add the selected backend configuration to the staged target request."""
        return {
            **prepared_files,
            "submission_key": submission_key,
            "backend": self.backend.name,
            "backend_options": dict(self.backend.options),
            "detached": self.backend.default_detached,
        }

    def _operation(self, name):
        """Select the target backend class, never a serialized Python object."""
        return BackendOperation.for_backend(type(self.backend), name)

    async def _invoke(self, name, *args, **kwargs):
        return await self.transport.call(self._operation(name), *args, **kwargs)

    @staticmethod
    def _context(client, staging):
        if not isinstance(staging, StagingSpec):
            staging = StagingSpec(**dict(staging))
        return _RunContext(client, staging)

    def _validate_run(self, run):
        if (run.execution.transport != self.transport or
                run.execution.backend != self.backend_config):
            raise ValueError("Run belongs to a different transport or backend")

    @staticmethod
    def _staging_from_manifest(files):
        mapping = files.mapping
        if mapping is None:
            return StagingSpec()
        return StagingSpec(
            local_root=mapping.local_root,
            remote_root=mapping.remote_root,
            input_sync=mapping.input_sync,
            output_sync=mapping.output_sync,
            filesystem_id=mapping.filesystem_id,
        )

    async def _resolve_submission_directory(self, directory):
        if self.transport.is_local:
            return os.path.abspath(directory)
        directory = posixpath.normpath(os.fspath(directory))
        if directory == "~":
            return await self._remote_home()
        if directory.startswith("~/"):
            return posixpath.join(await self._remote_home(), directory[2:])
        return directory

    def submission_directory(self, spec):
        if self.transport.is_local:
            return os.path.abspath(spec.directory)
        staging = spec.staging
        relative = _relative_local_path(
            spec.directory,
            staging.local_root or Path.home(),
        )

        root = staging.remote_root
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

        if self.transport.is_local:
            return record

        async def resolve():
            context = self._context(self, spec.staging)
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
                    filesystem_id=context.filesystem_id,
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
        return await self._invoke("submit", request)

    async def _status(self, directory, backend_id, files=None):
        return await self._invoke("status", backend_id)

    async def _cancel(self, directory, backend_id, files=None):
        return await self._invoke("cancel", backend_id)

    async def _follow(self, directory, backend_id, *, on_output, offsets=None,
                      stdout_path=None, stderr_path=None):
        offsets = {"out": 0, "err": 0} if offsets is None else offsets
        return await self.transport.follow(
            self._operation("follow"), backend_id, on_output,
            stdout_offset=offsets.get("out"), stderr_offset=offsets.get("err"),
            stdout_path=stdout_path, stderr_path=stderr_path,
        )

    def acknowledge_submission(self, run):
        """Release temporary submission identity after client persistence."""
        if BackendCapability.REATTACH not in self.backend.supports:
            return None
        key = run.execution.submission_key or run.id
        return _run_sync(self._invoke("acknowledge_submission", key))

    def acknowledge(self, run):
        """Release a delivered receipt only after durable RunRecord completion."""
        if BackendCapability.REATTACH not in self.backend.supports:
            return None
        self._validate_run(run)
        return _run_sync(self._invoke("acknowledge", run.execution.backend_id))

    def status(self, *runs):
        if BackendCapability.STATUS not in self.backend.supports:
            from kkrunner.backends.contracts import UnsupportedCapability
            raise UnsupportedCapability("This backend has no independent status query")
        for run in runs:
            self._validate_run(run)
            if not run.execution.backend_id:
                raise ValueError("Run lacks a backend identity")
        if not runs:
            return ()

        async def query():
            result = []
            for run in runs:
                execution = run.execution
                state = await self._status(execution.directory.target, execution.backend_id, run.files)
                result.append(
                    _run_status_from_facts(
                        state,
                        execution.backend_id,
                    )
                )
            return tuple(result)

        return _run_sync(query())

    async def _recover_submission(self, run_id, submission, *, submitted_at=None):
        directory = await self._resolve_submission_directory(submission.execution.directory.target)
        state = await self._invoke(
            "recover",
            run_id,
            {"backend": self.backend.name, "backend_options": dict(self.backend.options),
             "directory": directory},
            submitted_at=submitted_at,
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
            JobRef(self.backend.name, str(remote_id)),
            directory=state.get("directory") or directory,
            output_file=state.get("output_file"),
            error_file=state.get("error_file"),
        )

    def recover_submission(self, run_id, submission, *, submitted_at=None):
        if BackendCapability.RECOVER not in self.backend.supports:
            from kkrunner.backends.contracts import RecoveryResult
            return RecoveryResult.unsupported("Direct execution has no submission recovery")
        return _run_sync(self._recover_submission(run_id, submission, submitted_at=submitted_at))

    def file_session(self, spec):
        return FileSession(self._context(self, spec.staging), spec)

    def create_run_parts(self, spec, *, submission_key=None, callback=None):
        from uuid import uuid4
        spec.require_complete()
        return self._context(self, spec.staging).create_run_parts(
            spec, submission_key=submission_key or uuid4().hex, callback=callback,
        )

    def snapshot_file(self, run, file, *, destination=None):
        from kkrunner.client.file_access import FileAccess
        return FileAccess(self).snapshot_file(run, file, destination=destination)

    def fetch_result_files(self, run, files, *, destinations=None):
        from kkrunner.client.file_access import FileAccess
        return FileAccess(self).fetch_result_files(run, files, destinations=destinations)

    async def _open_run_parts(self, attachment, *, callback=None, output_listener=None):
        self._validate_run(attachment)
        staging = self._staging_from_manifest(attachment.files)
        return await self._context(self, staging).attach_run_parts(
            attachment, callback=callback, output_listener=output_listener,
        )

    def open_run_parts(self, attachment, *, callback=None, output_listener=None):
        self._require_managed()
        return _run_sync(
            self._open_run_parts(
                attachment, callback=callback, output_listener=output_listener,
            )
        )
