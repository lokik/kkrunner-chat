from kkrunner.backends.psij.submission import psij_state
import asyncio


import subprocess


import threading


from dataclasses import replace


from datetime import timedelta


from types import SimpleNamespace


import pytest


from kkrunner.runs import Execution, RunRecord, WorkingDirectory


from kkrunner.client.endpoint import ExecutionEndpointConfig


from kkrunner.client.contracts import JobAttachment


from kkrunner.backends.contracts import SubmissionContext




from kkrunner.files import FileEntry, FileManifest


import kkrunner.backends.psij.executor as psij_backend


from kkrunner.backends.psij import slurm as psij_slurm


from kkrunner.runs.base import ExecutionSpec, LaunchSpec, RunState


class _Result:
    def complete(self, error, return_code):
        self.error = error
        self.return_code = return_code


class _Parser:
    print_output = False

    async def read_output(self, stdout, result):
        result.output = await stdout.read()
        return result.output

    async def read_error(self, stderr, result):
        result.error_output = await stderr.read()
        return result.error_output

    def result(self, output, error, return_code):
        if return_code:
            raise ValueError(f"Process returned {return_code}")
        return (output, error, return_code)


class _Reader:
    print_output = False

    def __init__(self):
        self.parser = _Parser()

    def create_result(self, output_file, input_file):
        return _Result()

    async def parse_streams(self, stdout, stderr, result):
        return await asyncio.gather(self.parser.read_output(stdout, result), self.parser.read_error(stderr, result))

    def complete_result(self, result, output, error, return_code):
        _, error, return_code = self.parser.result(output, error, return_code)
        result.complete(error, return_code)
        return result


class _Status:
    def __init__(self, name="NEW", exit_code=None):
        self.state = SimpleNamespace(name=name)
        self.exit_code = exit_code


class _JobSpec:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _JobAttributes:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _ResourceSpecV1:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _Job:
    def __init__(self, spec=None):
        self.spec = spec
        self.native_id = None
        self._status = _Status()
        self._status_callback = None
        self._done = threading.Event()

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        self._status = status
        self.emit(status)

    def emit(self, status):
        if self._status_callback is not None:
            self._status_callback(self, status)

    def set_job_status_callback(self, callback):
        self._status_callback = callback

    def wait(self):
        self._done.wait(2)
        return self.status

    def cancel(self):
        self.status = _Status("CANCELED", 1)
        self._done.set()


class _Backend:
    def __init__(self):
        self.submitted = None
        self.attached = None

    def submit(self, job):
        self.submitted = job
        job.native_id = "123"
        job.status = _Status("QUEUED")

        def run():
            job.status = _Status("ACTIVE")
            if job.spec.stdout_path is not None:
                with open(job.spec.stdout_path, "wb") as out:
                    out.write(b"hello\n")
            if job.spec.stderr_path is not None:
                mode = "ab" if job.spec.stderr_path == job.spec.stdout_path else "wb"
                with open(job.spec.stderr_path, mode) as err:
                    err.write(b"warning\n")
            job.status = _Status("COMPLETED", 0)
            job._done.set()

        threading.Thread(target=run, daemon=True).start()

    def attach(self, job, native_id):
        self.attached = native_id
        job.native_id = native_id
        job.status = _Status("COMPLETED", 0)
        job._done.set()


class _JobExecutor:
    @staticmethod
    def get_instance(*args, **kwargs):
        raise AssertionError("A supplied backend should be used")


_FAKE_PSIJ = SimpleNamespace(
    Job=_Job, JobSpec=_JobSpec, JobAttributes=_JobAttributes, JobExecutor=_JobExecutor, ResourceSpecV1=_ResourceSpecV1
)


def _executor(monkeypatch, backend):
    monkeypatch.setattr(psij_backend, "_load_psij", lambda: _FAKE_PSIJ)
    return psij_backend.PsijExecutor("slurm", job_executor=backend)


def _psij_run(directory, backend_id=None):
    return RunRecord(
        id="psij-run",
        state=RunState.RUNNING,
        created_at="2026-10-13T08:00:00+00:00",
        updated_at="2026-10-13T08:00:00+00:00",
        execution=Execution(
            ExecutionEndpointConfig(executor="psij", psij_backend="slurm"), WorkingDirectory(str(directory)), backend_id
        ),
        files=FileManifest(
            inputs=(FileEntry(local_path=str(directory / "run.inp")),),
            stdout=FileEntry(local_path=str(directory / "run.out")),
            stderr=FileEntry(local_path=str(directory / "run.err")),
        ),
        metadata={"test": {"task": "DOS"}},
        processing_required=True,
    )


@pytest.mark.parametrize(
    "native_name, return_code, attached, expected",
    [
        ("COMPLETED", 7, False, RunState.FAILED),
        ("COMPLETED", 0, False, RunState.FINISHED),
        ("COMPLETED", None, False, RunState.FINISHED),
        ("FAILED", 0, False, RunState.FAILED),
        ("CANCELED", 1, False, RunState.CANCELLED),
        ("NEW", None, False, RunState.NEW),
        ("NEW", None, True, RunState.UNKNOWN),
        ("ACTIVE", None, True, RunState.RUNNING),
        (None, None, True, RunState.UNKNOWN),
    ],
)
def test_native_psij_state_normalization(native_name, return_code, attached, expected):
    native = None if native_name is None else _Status(native_name, return_code)
    assert psij_state(native, attached=attached) == expected


def test_psij_non_slurm_status_uses_shared_normalization(monkeypatch):
    executor = _executor(monkeypatch, _Backend())
    executor.backend = "pbs"
    executor.url = "remote-pbs"

    class Job:
        def __init__(self):
            self.status = _Status("COMPLETED", 9)

    class Backend:
        def attach(self, job, native_id):
            assert native_id == "pbs-123"

    executor._psij = SimpleNamespace(Job=Job)
    executor._job_executor = Backend()
    result = executor.status_native("pbs-123")
    assert result.state == RunState.FAILED
    assert result.return_code == 9
    assert result.failure_phase == psij_backend.FailurePhase.EXECUTION


def test_psij_nonzero_completed_job_is_failed(monkeypatch, tmp_path):
    backend = _Backend()
    executor = _executor(monkeypatch, backend)
    runner = executor.create_native_job(
        ExecutionSpec(
            argv=("program",), directory=str(tmp_path), input_file=None, output_file=str(tmp_path / "run.out")
        )
    )
    runner._job.status = _Status("COMPLETED", 7)
    assert runner.state == RunState.FAILED


def test_psij_slurm_nonzero_accounted_job_is_failed(monkeypatch):
    executor = _executor(monkeypatch, _Backend())
    monkeypatch.setattr(psij_slurm, "_slurm_details", lambda native_id: {})
    monkeypatch.setattr(psij_slurm, "_scheduler_output", lambda argv: "123|COMPLETED|7:0|\\n".replace("\\n", "\n"))
    status = executor.status_native("123")
    assert status.state == RunState.FAILED
    assert status.return_code == 7
    assert status.failure_phase == psij_backend.FailurePhase.EXECUTION


@pytest.mark.parametrize("mpi, process_count", [(16, 16), ("auto", None)])
def test_psij_mpi_job_uses_mpi_executable_and_requested_resources(monkeypatch, tmp_path, mpi, process_count):
    executor = _executor(monkeypatch, _Backend())
    input_path = tmp_path / "run.inp"
    with input_path.open("w+b") as input_file, (tmp_path / "run.out").open("wb") as output_file:
        spec = ExecutionSpec(
            argv=("program",),
            mpi_argv=("programMPI", str(input_path)),
            launch=LaunchSpec.from_mpi(mpi),
            input_file=input_file,
            output_file=output_file,
            directory=str(tmp_path),
            stdin=input_file,
        )
        runner = executor.create_native_job(spec)
        job_spec = runner._job.spec
        assert job_spec.executable == "programMPI"
        assert job_spec.arguments == [str(input_path)]
        assert job_spec.launcher == "srun"
        assert job_spec.stdin_path == str(input_path)
        if process_count is None:
            assert job_spec.resources is None
        else:
            assert job_spec.resources.process_count == process_count


def test_psij_status_is_nonblocking_and_explicit_wait_synchronizes(monkeypatch):
    executor = _executor(monkeypatch, _Backend())
    executor.backend = "pbs"
    executor.url = "remote-pbs"
    observed = []

    class Job:
        def __init__(self):
            self.status = _Status("NEW")

        def wait(self, *, timeout, target_states):
            observed.append((timeout, target_states))
            self.status = _Status("ACTIVE")
            return self.status

    class Backend:
        def attach(self, job, native_id):
            observed.append(native_id)

    executor._job_executor = Backend()
    executor._psij = SimpleNamespace(Job=Job, JobState=SimpleNamespace(QUEUED="QUEUED"))
    status = executor.status_native("native-42")
    assert status.state == RunState.UNKNOWN
    assert observed == ["native-42"]
    status = executor.wait_status_native("native-42", timeout=2.5)
    assert status.state == RunState.RUNNING
    assert observed == ["native-42", (timedelta(seconds=2.5), ["QUEUED"])]
    assert executor.status_native("native-42").state == RunState.RUNNING
    assert len(observed) == 2


def test_psij_explicit_wait_timeout_keeps_cached_new_unknown(monkeypatch):
    executor = _executor(monkeypatch, _Backend())
    executor.backend = "pbs"
    executor.url = "remote-pbs"
    observed = []

    class Job:
        def __init__(self):
            self.status = _Status("NEW")

        def wait(self, *, timeout, target_states):
            observed.append((timeout, target_states))
            return None

    class Backend:
        def attach(self, job, native_id):
            observed.append(native_id)

    executor._job_executor = Backend()
    executor._psij = SimpleNamespace(Job=Job, JobState=SimpleNamespace(QUEUED="QUEUED"))
    first = executor.status_native("native-42")
    assert first.state == RunState.UNKNOWN
    assert observed == ["native-42"]
    status = executor.wait_status_native("native-42", timeout=0.05)
    assert status.state == RunState.UNKNOWN
    assert status.backend_id == "native-42"
    assert executor.status_native("native-42").state == RunState.UNKNOWN
    assert observed == ["native-42", (timedelta(seconds=0.05), ["QUEUED"])]
    assert executor.wait_status_native("native-42", timeout=0).state == RunState.UNKNOWN
    assert len(observed) == 2
    with pytest.raises(ValueError, match="negative"):
        executor.wait_status_native("native-42", timeout=-1)


def test_psij_local_slurm_explicit_wait_uses_native_job_wait(monkeypatch):
    executor = _executor(monkeypatch, _Backend())
    observed = []

    class Job:
        def wait(self, **kwargs):
            observed.append(("wait", kwargs))

    class Backend:
        def attach(self, job, native_id):
            observed.append(("attach", native_id))

    executor._psij = SimpleNamespace(Job=Job, JobState=SimpleNamespace(QUEUED="QUEUED"))
    executor._job_executor = Backend()
    states = [RunState.UNKNOWN, RunState.UNKNOWN, RunState.RUNNING]
    monkeypatch.setattr(
        executor, "status_native", lambda native_id: psij_backend.RunStatus(states.pop(0), backend_id=native_id)
    )
    assert executor.wait_status_native("slurm-123", timeout=0).state == RunState.UNKNOWN
    assert executor.wait_status_native("slurm-123", timeout=0.1).state == RunState.RUNNING
    assert observed[0] == ("attach", "slurm-123")
    assert observed[1][0] == "wait"


def test_psij_uncertain_submit_without_native_id_stays_recoverable(monkeypatch, tmp_path):
    from kkrunner.runs.base import SubmissionNotStarted

    class FailingBackend(_Backend):
        def submit(self, job):
            raise OSError("scheduler reply lost")

    executor = _executor(monkeypatch, FailingBackend())
    runner = executor.create_native_job(
        ExecutionSpec(
            argv=("program",), directory=str(tmp_path), input_file=None, output_file=str(tmp_path / "run.out")
        ),
        submission_key="run-123",
    )
    with pytest.raises(OSError, match="scheduler reply lost") as caught:
        runner.submit_native()
    assert not isinstance(caught.value, SubmissionNotStarted)
    assert runner.backend_id is None
    assert runner.failure_phase is None


def test_psij_preparation_failure_is_definitively_not_submitted(monkeypatch, tmp_path):
    from kkrunner.runs.base import FailurePhase, SubmissionNotStarted

    executor = _executor(monkeypatch, _Backend())
    runner = executor.create_native_job(
        ExecutionSpec(
            argv=("program",), directory=str(tmp_path), input_file=None, output_file=str(tmp_path / "run.out")
        ),
        submission_key="run-123",
    )

    def fail_preparation():
        raise OSError("cannot prepare output")

    monkeypatch.setattr(runner, "_prepare_io", fail_preparation)
    with pytest.raises(SubmissionNotStarted, match="cannot prepare output"):
        runner.submit_native()
    assert executor._job_executor.submitted is None
    assert runner.failure_phase == FailurePhase.SUBMISSION


def test_psij_native_attachment_and_status_share_one_job(monkeypatch):

    class Backend:
        def __init__(self):
            self.attaches = []

        def attach(self, job, native_id):
            self.attaches.append((job, native_id))
            job.native_id = native_id
            job.status = _Status("ACTIVE")

    backend = Backend()
    executor = _executor(monkeypatch, backend)
    executor.backend = "pbs"
    executor.url = "remote-pbs"
    first = executor.attach_native_job("native-42")
    second = executor.attach_native_job("native-42")
    assert first is second
    assert executor.status_native("native-42").state == RunState.RUNNING
    assert executor.attach_native_job("native-42") is first
    assert backend.attaches == [(first, "native-42")]


def test_psij_recovers_live_slurm_submission_by_run_id(monkeypatch, tmp_path):
    executor = _executor(monkeypatch, _Backend())
    calls = []

    def scheduler_output(argv):
        calls.append(tuple(argv))
        if argv[0] == "squeue":
            return f"123|{psij_slurm.submission_marker('run-1')}|RUNNING|{tmp_path}\n"
        if argv[:4] == ("scontrol", "--oneliner", "show", "job"):
            return f"JobId=123 WorkDir={tmp_path} StdOut={tmp_path / 'run.out'} StdErr={tmp_path / 'run.err'}\n"
        raise AssertionError(argv)

    monkeypatch.setattr(psij_slurm, "_scheduler_output", scheduler_output)
    status = _recovery_endpoint(executor, monkeypatch).recover_submission("run-1", SubmissionContext(_psij_run(tmp_path).execution))
    assert status.ref.backend == "psij"
    assert status.ref.token == "123"
    assert status.directory == str(tmp_path)
    assert status.output_file == str(tmp_path / "run.out")
    assert status.error_file == str(tmp_path / "run.err")
    assert calls[0][0] == "squeue"


@pytest.mark.parametrize(
    "exit_code, accounting_flags, use_workdir_paths, expected_state",
    [(0, "job_comment,job_env", True, RunState.FINISHED), (7, "job_comment", False, RunState.FAILED)],
)
def test_psij_recovers_completed_slurm_submission_from_accounting(
    monkeypatch, tmp_path, exit_code, accounting_flags, use_workdir_paths, expected_state
):
    executor = _executor(monkeypatch, _Backend())

    def scheduler_output(argv):
        if argv[0] == "squeue":
            return ""
        if argv[:3] == ("scontrol", "show", "config"):
            return f"AccountingStoreFlags = {accounting_flags}\n"
        if argv[0] == "sacct":
            directory, stdout, stderr = (
                (str(tmp_path), str(tmp_path / "run.out"), str(tmp_path / "run.err"))
                if use_workdir_paths
                else ("/tmp", "out", "err")
            )
            return f"456|{psij_slurm.submission_marker('run-2')}|COMPLETED|{directory}|{stdout}|{stderr}|{exit_code}:0|\n"
        raise AssertionError(argv)

    monkeypatch.setattr(psij_slurm, "_scheduler_output", scheduler_output)
    status = _recovery_endpoint(executor, monkeypatch).recover_submission("run-2", SubmissionContext(_psij_run(tmp_path).execution), submitted_at="2026-10-13T08:00:00+02:00")
    assert status.ref.token == "456"
    assert status.directory == (str(tmp_path) if use_workdir_paths else "/tmp")
    facts = executor.recover_native(
        "run-2", str(tmp_path), submitted_at="2026-10-13T08:00:00+02:00",
    )
    assert facts["state"] == expected_state
    assert facts["return_code"] == exit_code
    if use_workdir_paths:
        assert status.output_file == str(tmp_path / "run.out")


@pytest.mark.parametrize(
    "accounting_flags, accounting_available, run_id, expected_error",
    [("job_comment", True, "missing", "still uncertain"), ("job_env", False, "uncertain", "comments are unavailable")],
)
def test_psij_unavailable_accounting_keeps_submission_uncertain(
    monkeypatch, tmp_path, accounting_flags, accounting_available, run_id, expected_error
):
    executor = _executor(monkeypatch, _Backend())

    def scheduler_output(argv):
        if argv[0] == "squeue":
            return ""
        if argv[:3] == ("scontrol", "show", "config"):
            return f"AccountingStoreFlags = {accounting_flags}\n"
        if argv[0] == "sacct" and accounting_available:
            return ""
        raise AssertionError(argv)

    monkeypatch.setattr(psij_slurm, "_scheduler_output", scheduler_output)
    with pytest.raises(NotImplementedError, match=expected_error):
        _recovery_endpoint(executor, monkeypatch).recover_submission(run_id, SubmissionContext(_psij_run(tmp_path).execution), submitted_at="2026-10-13T08:00:00+02:00")


def test_psij_remote_url_does_not_search_local_scheduler(monkeypatch, tmp_path):
    executor = _executor(monkeypatch, _Backend())
    executor.url = "ssh://other-cluster"
    monkeypatch.setattr(psij_slurm, "_scheduler_output", lambda argv: pytest.fail("must not search local Slurm"))
    with pytest.raises(NotImplementedError, match="local Slurm"):
        executor.recover_native("run-remote", str(tmp_path))


def test_psij_recovery_handles_job_disappearing_between_queries(monkeypatch, tmp_path):
    executor = _executor(monkeypatch, _Backend())

    def scheduler_output(argv):
        if argv[0] == "squeue":
            return f"123|{psij_slurm.submission_marker('run-race')}|RUNNING|{tmp_path}\n"
        if argv[:4] == ("scontrol", "--oneliner", "show", "job"):
            raise subprocess.CalledProcessError(1, argv)
        if argv[:3] == ("scontrol", "show", "config"):
            return "AccountingStoreFlags = job_comment\n"
        if argv[0] == "sacct":
            return f"123|{psij_slurm.submission_marker('run-race')}|COMPLETED|{tmp_path}|{tmp_path / 'run.out'}|{tmp_path / 'run.err'}|0:0|\n"
        raise AssertionError(argv)

    monkeypatch.setattr(psij_slurm, "_scheduler_output", scheduler_output)
    facts = executor.recover_native("run-race", str(tmp_path), submitted_at="2026-10-13T08:00:00+02:00")
    assert facts["native_id"] == "123"
    assert facts["state"] == RunState.FINISHED
    assert facts["return_code"] == 0


def test_psij_recovery_rejects_ambiguous_live_jobs(monkeypatch, tmp_path):
    executor = _executor(monkeypatch, _Backend())
    monkeypatch.setattr(
        psij_slurm,
        "_scheduler_output",
        lambda argv: (
            f"123|{psij_slurm.submission_marker('duplicate')}|RUNNING|/one\n"
            f"124|{psij_slurm.submission_marker('duplicate')}|PENDING|/two\n"
        ),
    )
    with pytest.raises(RuntimeError, match="Multiple live Slurm jobs"):
        _recovery_endpoint(executor, monkeypatch).recover_submission("duplicate", SubmissionContext(_psij_run(tmp_path).execution))


def test_psij_submit_is_serialized_across_threads(monkeypatch, tmp_path):

    class BlockingBackend(_Backend):
        def __init__(self):
            super().__init__()
            self.calls = 0
            self.first_entered = threading.Event()
            self.second_entered = threading.Event()
            self.release = threading.Event()

        def submit(self, job):
            self.calls += 1
            (self.first_entered if self.calls == 1 else self.second_entered).set()
            self.release.wait(1)
            return super().submit(job)

    backend = BlockingBackend()
    executor = _executor(monkeypatch, backend)
    input_file = open(tmp_path / "run.inp", "wb+")
    input_file.write(b"input")
    output_file = open(tmp_path / "run.out", "wb")
    spec = ExecutionSpec(
        argv=("program",), input_file=input_file, output_file=output_file, directory=str(tmp_path), stdin=input_file
    )
    runner = executor.create_native_job(spec)
    errors = []

    def submit():
        try:
            runner.submit_native()
        except RuntimeError as exc:
            errors.append(exc)

    first = threading.Thread(target=submit)
    second = threading.Thread(target=submit)
    first.start()
    assert backend.first_entered.wait(1)
    second.start()
    assert not backend.second_entered.wait(0.2)
    backend.release.set()
    first.join(2)
    second.join(2)
    assert not first.is_alive()
    assert not second.is_alive()
    assert backend.calls == 1
    assert len(errors) == 1
    runner.wait()
    input_file.close()


def test_psij_mpi_respects_explicit_stdin_policy(monkeypatch, tmp_path):
    backend = _Backend()
    executor = _executor(monkeypatch, backend)
    spec = ExecutionSpec(
        argv=("program",),
        mpi_argv=("parallel",),
        launch=LaunchSpec.from_mpi(2),
        input_file=None,
        output_file=None,
        directory=str(tmp_path),
        stdin=str(tmp_path / "stdin.dat"),
        mpi_uses_stdin=False,
    )
    runner = executor.create_native_job(spec)
    assert runner._job.spec.stdin_path is None
    assert replace(spec, mpi_uses_stdin=True).mpi_uses_stdin


@pytest.mark.parametrize("merged", (False, True))
def test_psij_preparation_closes_each_output_handle_once(monkeypatch, tmp_path, merged):

    class NamedOutput:
        def __init__(self, name):
            self.name = str(name)
            self.close_calls = 0
            self.flush_calls = 0

        def flush(self):
            self.flush_calls += 1

        def close(self):
            self.close_calls += 1
            if self.close_calls != 1:
                raise AssertionError("Output handle was closed more than once")

    executor = _executor(monkeypatch, _Backend())
    output = NamedOutput(tmp_path / "run.out")
    error = output if merged else NamedOutput(tmp_path / "run.err")
    runner = executor.create_native_job(
        ExecutionSpec(argv=("program",), directory=str(tmp_path), input_file=None, output_file=output, error_file=error)
    )
    runner._prepare_io()
    assert output.close_calls == 1
    assert error.close_calls == 1
    assert output.flush_calls >= 1
    assert error.flush_calls >= 1
    assert (tmp_path / "run.out").exists()
    if not merged:
        assert (tmp_path / "run.err").exists()


def test_psij_active_job_stays_active_without_blocking_wait(monkeypatch, tmp_path):
    executor = _executor(monkeypatch, _Backend())
    runner = executor.create_native_job(
        ExecutionSpec(argv=("program",), directory=str(tmp_path), input_file=None, output_file=None)
    )
    runner._job.status = _Status("ACTIVE")
    assert runner.state is RunState.RUNNING


def test_psij_confirmed_completion_is_not_undone_by_unknown_refresh(monkeypatch, tmp_path):
    from kkrunner.runs.base import RunStatus
    from kkrunner.runs.manager import RunManager
    from kkrunner.runs.records import RunStore

    _executor(monkeypatch, _Backend())
    manager = RunManager(RunStore(tmp_path / "history"))
    record = manager.new_record(
        directory=tmp_path, endpoint=None, metadata={"test": {"task": "DOS"}}, processing_required=True
    )
    manager.store.save(record)
    record = manager.transition(record, state=RunState.RUNNING)
    record = manager.update(record, RunStatus(RunState.FINISHED))
    assert record.state is RunState.POSTPROCESSING
    older = manager.update(record, RunStatus(RunState.UNKNOWN, message="disconnected"))
    assert older.state is RunState.POSTPROCESSING
    assert older.message is None


def test_psij_status_only_cache_is_bounded(monkeypatch):
    backend = _Backend()
    executor = _executor(monkeypatch, backend)
    executor.backend = "pbs"
    executor.url = "remote-pbs"
    for number in range(90):
        executor.attach_native_job(str(number))
    assert len(executor._jobs) <= executor._max_idle_jobs


def test_psij_preparation_reuses_declared_outputs_for_check_and_cleanup(monkeypatch, tmp_path):
    """Streams and declared wildcard outputs are cleaned, inputs/caches kept."""
    input_file = tmp_path / "run.inp"
    cache_file = tmp_path / "restart.pot"
    output_file = tmp_path / "run.out"
    error_file = tmp_path / "run.err"
    extra_output = tmp_path / "result-1.dat"
    for path in (input_file, cache_file, output_file, error_file, extra_output):
        path.write_bytes(b"previous contents")
    executor = _executor(monkeypatch, _Backend())
    runner = executor.create_native_job(
        ExecutionSpec(
            argv=("program",),
            directory=str(tmp_path),
            input_file=str(input_file),
            output_file=str(output_file),
            error_file=str(error_file),
            output_files=("result-*.dat", "run.out"),
            cache_files=("restart.pot",),
        )
    )
    runner._prepare_io()
    assert input_file.read_bytes() == b"previous contents"
    assert cache_file.read_bytes() == b"previous contents"
    assert output_file.read_bytes() == b""
    assert error_file.read_bytes() == b""
    assert not extra_output.exists()


def test_remote_psij_submit_does_not_start_client_monitor(monkeypatch, tmp_path):
    from kkrunner.backends.psij import operations as agent

    calls = []
    class FakeRunner:
        backend_id = "123"
        state = RunState.RUNNING
        result = SimpleNamespace(output_file="out", error_file="err")
        return_code = None
        failure_phase = None

        def submit_native(self):
            calls.append("submit-native")

        def submit(self):
            raise AssertionError("The target must not create a client monitor")

    class FakeExecutor:
        backend = "slurm"
        url = None
        def create_native_job(self, spec, *, submission_key=None):
            return FakeRunner()

    monkeypatch.setattr(agent, "_psij_executor", lambda opts: FakeExecutor())
    request = {
        "argv": ["true"], "directory": str(tmp_path), "executor": "psij",
        "executor_options": {"backend": "slurm"}, "id": "test",
    }
    result = agent._submit_impl(request)
    assert calls == ["submit-native"]
    assert result["state"] == "running"


def _recovery_endpoint(executor, monkeypatch):
    from kkrunner.backends import PsijBackend
    from kkrunner.client.local import LocalRunEndpoint
    from kkrunner.backends.psij import operations
    monkeypatch.setattr(operations, "_psij_executor", lambda options: executor)
    return LocalRunEndpoint(PsijBackend, backend_options={"backend": executor.backend, "url": executor.url})
