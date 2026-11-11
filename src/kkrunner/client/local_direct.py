"""Client adapter for a locally started direct subprocess.

This is the same DirectSession lifecycle used by SSH. Only process launching
and local output stream ownership differ from the SSH transport.
"""
from __future__ import annotations

from kkrunner.backends.support import materialize_local_spec, start_local_process, stop_local_process
from kkrunner.files.availability import prepare_target_outputs
from kkrunner.files.paths import file_path
from kkrunner.files.streams import open_output_streams


class LocalProcessAdapter:
    """Direct subprocess mechanism, separate from common Job lifecycle."""

    expose_pid = True
    uncertain_after_launch = False

    def __init__(self):
        self._streams = None
        self._detached = False

    @property
    def detached(self):
        return self._detached

    def configure_detached(self, detached):
        self._detached = detached

    def configure_submission(self, request, *, stream_offsets, stream_writer, stream_paths):
        raise RuntimeError("Local direct execution has no remote staging request")

    def prepare(self, spec):
        def paths(values):
            return [path for value in values
                    if (path := file_path(value, spec.directory)) is not None]

        prepare_target_outputs({
            "input_file": file_path(spec.input_file, spec.directory),
            "stdin": file_path(spec.stdin, spec.directory),
            "input_files": paths(spec.input_files),
            "output_file": file_path(spec.output_file, spec.directory),
            "error_file": file_path(spec.error_file, spec.directory),
            "client_sync": {
                "output_files": paths(spec.output_files),
                "cache_files": [{"path": path} for path in paths(spec.cache_files)],
            },
        })
        self._streams = open_output_streams(spec)

    async def launch(self, spec):
        # Resolve MPI on the execution host, not while constructing the Job.
        prepared = materialize_local_spec(spec)
        if self._detached:
            import asyncio
            import os
            # No child descriptor may rely on a client pipe. Named streams
            # write directly into files; absent streams go to DEVNULL.
            for value in (spec.output_file, spec.error_file):
                if value is not None and not isinstance(value, (str, os.PathLike)):
                    raise ValueError("Detached direct output requires a named file or None")
            return await start_local_process(
                prepared,
                stdin=asyncio.subprocess.DEVNULL if prepared.stdin is None else prepared.stdin,
                stdout=self._streams.output_file or asyncio.subprocess.DEVNULL,
                stderr=self._streams.error_file or asyncio.subprocess.DEVNULL,
                start_new_session=True,
            )
        return await start_local_process(prepared)

    async def stop(self, process):
        await stop_local_process(process, process_group=self._detached)
        return True

    def detached_output_paths(self, spec):
        if not self._detached:
            return {}
        return {
            kind: path
            for kind, value in (("out", spec.output_file), ("err", spec.error_file))
            if (path := file_path(value, spec.directory)) is not None
        }

    def output(self, data, kind):
        target = (
            self._streams.error_file if kind == "err" else self._streams.output_file
        )
        if target is not None:
            target.write(data)
            target.flush()

    def exit_status(self, code):
        return None  # Common Job interprets ordinary exit codes.

    def close(self, process):
        streams, self._streams = self._streams, None
        if streams is not None:
            streams.close()


