"""Local process launch, output and file cleanup for the shared direct Job."""

from __future__ import annotations

from kkrunner.backends.process.primitives import start_local_process, stop_local_process
from kkrunner.files.availability import clear_declared_outputs, discover_local_files, validate_output_collisions
from kkrunner.files.manifest import FileManifest
from kkrunner.files.paths import file_path
from kkrunner.runs.base import FailurePhase, RunState, close_output_files, materialize_output_streams


class LocalProcessAdapter:
    """Direct subprocess mechanism, separate from common Job lifecycle."""

    expose_pid = True
    uncertain_after_launch = False

    def configure_submission(self, request, *, stream_offsets, stream_writer, stream_paths):
        raise RuntimeError("Local direct execution has no remote staging request")

    def prepare(self, job):
        spec = job.spec
        protected = [
            file_path(value, spec.directory)
            for value in (spec.input_file, spec.stdin, *spec.input_files, *spec.cache_files)
        ]
        outputs = [file_path(value, spec.directory) for value in spec.output_files]
        validate_output_collisions(
            (*outputs, job.result.output_file, job.result.error_file),
            protected=protected,
        )
        job.spec = materialize_output_streams(spec)
        clear_declared_outputs(
            outputs,
            protected=(*protected, job.result.output_file, job.result.error_file),
        )

    async def launch(self, job):
        return await start_local_process(job.spec)

    async def stop(self, job, process):
        await stop_local_process(process)
        return True

    def output(self, job, data, kind):
        target = job.spec.error_file if kind == "err" else job.spec.output_file
        if target is not None:
            target.write(data)
            target.flush()

    def exit_status(self, job, code):
        return None  # Common Job interprets ordinary exit codes.

    def verify(self, job):
        files = discover_local_files(FileManifest.from_spec(job.spec))
        if files.failed_artifacts:
            job._state = RunState.FAILED
            job._failure_phase = FailurePhase.ARTIFACTS
            job._failure_message = files.availability_message

    def close(self, job):
        close_output_files(job.spec.output_file, job.spec.error_file)
