"""Local process launch, output and file cleanup for the shared direct Job."""

from __future__ import annotations

from kkrunner.backends.process.primitives import materialize_local_spec, start_local_process, stop_local_process
from kkrunner.files.availability import prepare_target_outputs
from kkrunner.files.paths import file_path
from kkrunner.files.streams import open_output_streams


class LocalProcessAdapter:
    """Direct subprocess mechanism, separate from common Job lifecycle."""

    expose_pid = True
    uncertain_after_launch = False

    def __init__(self):
        self._streams = None

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
        return await start_local_process(materialize_local_spec(spec))

    async def stop(self, process):
        await stop_local_process(process)
        return True

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
