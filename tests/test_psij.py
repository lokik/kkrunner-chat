"""Native PSI/J submission, state, follow and recovery contract tests.

The backend implements static target operations. Client sessions, persistence
and file transfers are covered by their own suites; none of those mechanisms
requires a legacy PSI/J executor wrapper.
"""

import asyncio
import io
import subprocess
from types import SimpleNamespace

import pytest

from kkrunner import ExecutionSpec, RunState
from kkrunner.backends import PsijBackend
from kkrunner.backends.contracts import JobFailurePhase, JobStatus, SubmissionNotStarted
from kkrunner.backends.implementations.psij import backend as operations
from kkrunner.backends.implementations.psij import slurm
from kkrunner.backends.implementations.psij.submission import prepare_job, prepare_outputs
from kkrunner.backends.implementations.psij.support import psij_state, supports_recovery
from kkrunner.client.execution_request import execution_request
from kkrunner.execution import MpiSpec
from kkrunner.remote.handles import decode_handle, encode_handle


class NativeStatus:
    def __init__(self, name="NEW", exit_code=None, message=None):
        self.state = SimpleNamespace(name=name)
        self.exit_code = exit_code
        self.message = message


class NativeJobSpec:
    def __init__(self, **values):
        self.__dict__.update(values)


class NativeJobAttributes:
    def __init__(self, **values):
        self.__dict__.update(values)


class NativeResources:
    def __init__(self, **values):
        self.__dict__.update(values)


class NativeJob:
    def __init__(self, spec=None):
        self.spec = spec
        self.native_id = None
        self.status = NativeStatus()
        self.callback = None

    def set_job_status_callback(self, callback):
        self.callback = callback

    def update(self, state, code=None, message=None):
        self.status = NativeStatus(state, code, message)
        if self.callback is not None:
            self.callback(self, self.status)

    def cancel(self):
        self.update("CANCELED", 1)


PSIJ = SimpleNamespace(
    Job=NativeJob, JobSpec=NativeJobSpec,
    JobAttributes=NativeJobAttributes, ResourceSpecV1=NativeResources,
)


class NativeExecutor:
    def __init__(self, *, error=None):
        self.error = error
        self.submitted = []
        self.attached = []

    def submit(self, job):
        self.submitted.append(job)
        if self.error is not None:
            raise self.error
        job.native_id = "123"
        job.update("QUEUED")

    def attach(self, job, native_id):
        self.attached.append((job, native_id))
        job.native_id = native_id
        job.update("ACTIVE")


def request_at(tmp_path, *, spec=None):
    if spec is None:
        spec = ExecutionSpec(("program",), directory=str(tmp_path),
                             output_file=str(tmp_path / "run.out"))
    return execution_request(spec)


def handle(native_id="123", executor_name="slurm", url=None):
    return encode_handle({
        "kind": "psij", "native_id": native_id,
        "executor_name": executor_name, "url": url,
    })


@pytest.mark.parametrize(
    "native, code, attached, expected",
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
def test_native_status_normalization(native, code, attached, expected):
    status = None if native is None else NativeStatus(native, code)
    assert psij_state(status, attached=attached) is expected


def test_native_status_unknown_state_preserves_exit_code():
    assert psij_state(NativeStatus("FUTURE_STATE", None)) is RunState.UNKNOWN


def test_status_uses_native_attach_for_non_slurm_scheduler(monkeypatch):
    native = NativeJob()
    native.update("COMPLETED", 9, "native failure")
    calls = []

    def attach(ref):
        calls.append(ref)
        return native

    monkeypatch.setattr("kkrunner.backends.implementations.psij.support._attach_job", attach)
    (facts,) = PsijBackend.status([handle("pbs-123", "pbs")])
    assert calls == [{
        "kind": "psij", "native_id": "pbs-123",
        "executor_name": "pbs", "url": None,
    }]
    assert facts["state"] == "failed"
    assert facts["return_code"] == 9
    assert facts["failure_phase"] == "execution"
    assert facts["error"] == "native failure"


def test_local_slurm_status_does_not_need_psij_installation(monkeypatch):
    calls = []
    def scheduler_status(native_id):
        calls.append(native_id)
        return JobStatus(
            RunState.FAILED, return_code=7,
            failure_phase=JobFailurePhase.EXECUTION,
        )
    monkeypatch.setattr(slurm, "status", scheduler_status)
    (facts,) = PsijBackend.status([handle("native-7")])
    assert calls == ["native-7"]
    assert facts == {
        "state": "failed", "return_code": 7,
        "failure_phase": "execution", "error": None,
    }


@pytest.mark.parametrize("kind", ("status", "cancel"))
def test_native_operations_reject_other_backend_handles(kind):
    with pytest.raises(ValueError):
        getattr(PsijBackend, kind)(["not-a-valid-handle"] if kind == "status" else "not-a-valid-handle")


def test_submit_returns_native_identity_without_creating_client_monitor(monkeypatch, tmp_path):
    executor = NativeExecutor()
    monkeypatch.setattr(operations, "_create_executor", lambda options: (PSIJ, executor))
    facts = PsijBackend.submit(request_at(tmp_path))
    assert len(executor.submitted) == 1
    native = executor.submitted[0].spec
    key = native.environment["KKRunner_RUN_ID"]
    assert isinstance(key, str) and key
    assert native.attributes.custom_attributes == {"slurm.comment": slurm.submission_marker(key)}
    assert decode_handle(facts["remote_id"])["native_id"] == "123"
    assert facts["state"] == "queued"
    assert facts["directory"] == str(tmp_path)
    assert facts["output_file"] == str(tmp_path / "run.out")
    assert (tmp_path / "run.out").read_bytes() == b""


def test_scheduler_submission_error_is_uncertain_after_boundary(monkeypatch, tmp_path):
    executor = NativeExecutor(error=OSError("scheduler reply lost"))
    monkeypatch.setattr(operations, "_create_executor", lambda options: (PSIJ, executor))
    with pytest.raises(OSError, match="scheduler reply lost") as caught:
        PsijBackend.submit(request_at(tmp_path))
    assert not isinstance(caught.value, SubmissionNotStarted)
    assert len(executor.submitted) == 1


def test_preparation_error_is_definitively_not_submitted(monkeypatch, tmp_path):
    executor = NativeExecutor()
    monkeypatch.setattr(operations, "_create_executor", lambda options: (PSIJ, executor))
    monkeypatch.setattr(operations, "prepare_outputs",
                        lambda request: (_ for _ in ()).throw(OSError("cannot prepare output")))
    with pytest.raises(SubmissionNotStarted, match="cannot prepare output"):
        PsijBackend.submit(request_at(tmp_path))
    assert executor.submitted == []


@pytest.mark.parametrize("process_count", (None, 16))
def test_psij_mpi_job_uses_scheduler_launcher_and_resources(tmp_path, process_count):
    spec = ExecutionSpec(
        ("serial",), mpi_argv=("parallel", "input.inp"),
        mpi=MpiSpec(required=True, autodetect=True, process_count=process_count),
        directory=str(tmp_path), stdin="input.inp",
    )
    job = prepare_job(PSIJ, request_at(tmp_path, spec=spec),
                      executor_name="slurm", submission_key="run-123")
    native = job.spec
    assert native.executable == "parallel"
    assert native.arguments == ["input.inp"]
    assert native.launcher == "srun"
    assert native.stdin_path == str(tmp_path / "input.inp")
    assert (native.resources is None) is (process_count is None)
    if process_count is not None:
        assert native.resources.process_count == process_count
    assert native.environment["KKRunner_RUN_ID"] == "run-123"


def test_mpi_stdin_can_be_disabled_independently(tmp_path):
    spec = ExecutionSpec(
        ("serial",), mpi_argv=("parallel",),
        mpi=MpiSpec(required=True, autodetect=True, process_count=2),
        mpi_uses_stdin=False, stdin="input.inp", directory=str(tmp_path),
    )
    native = prepare_job(PSIJ, request_at(tmp_path, spec=spec),
                         executor_name="slurm", submission_key="run-123").spec
    assert native.stdin_path is None


def test_scheduler_explicit_launcher_does_not_probe_compute_node_binary(tmp_path):
    spec = ExecutionSpec(
        ("serial",), mpi_argv=("/compute-only/programMPI", "input.inp"),
        mpi=MpiSpec(required=True, launcher=("/compute-only/mpirun",)),
        directory=str(tmp_path),
    )
    job = prepare_job(PSIJ, request_at(tmp_path, spec=spec),
                      executor_name="slurm", submission_key="run-123")
    assert job.spec.executable == "/compute-only/mpirun"
    assert job.spec.arguments == ["/compute-only/programMPI", "input.inp"]


@pytest.mark.parametrize("executor_name,launcher", (("slurm", "srun"), ("pbs", "mpirun")))
def test_scheduler_selection_changes_mpi_launcher(tmp_path, executor_name, launcher):
    spec = ExecutionSpec(
        ("serial",), mpi_argv=("parallel",),
        mpi=MpiSpec(required=True, autodetect=True), directory=str(tmp_path),
    )
    job = prepare_job(PSIJ, request_at(tmp_path, spec=spec),
                      executor_name=executor_name, submission_key="run-123")
    assert job.spec.launcher == launcher
    assert (job.spec.attributes is None) is (executor_name != "slurm")


def test_psij_output_preparation_preserves_cache_and_cleans_stale_results(tmp_path):
    cache = tmp_path / "restart.pot"
    stale = tmp_path / "old.pot"
    stdout = tmp_path / "run.out"
    stderr = tmp_path / "run.err"
    for file in (cache, stale, stdout, stderr):
        file.write_text("previous")
    request = request_at(tmp_path, spec=ExecutionSpec(
        ("program",), directory=str(tmp_path),
        output_file=str(stdout), error_file=str(stderr),
        output_files=("*.pot",), cache_files=("restart.pot",),
    ))
    prepare_outputs(request)
    assert cache.read_text() == "previous"
    assert not stale.exists()
    assert stdout.read_bytes() == b""
    assert stderr.read_bytes() == b""


def test_cancel_requests_native_job_but_does_not_invent_completion(monkeypatch):
    native = NativeJob()
    native.update("ACTIVE")
    monkeypatch.setattr(operations, "_attach_job", lambda identity: native)
    # The backend observes the native scheduler status after cancellation.
    monkeypatch.setattr(PsijBackend, "status", staticmethod(lambda identities: [{
        "state": "unknown", "return_code": None, "failure_phase": None, "error": None,
    } for identity in identities]))
    assert PsijBackend.cancel(handle("pbs-1", "pbs"))["state"] == "unknown"
    assert native.status.state.name == "CANCELED"


@pytest.mark.parametrize(
    "options, expected",
    [
        ({"executor_name": "slurm", "url": None}, True),
        ({"executor_name": "SLURM", "url": None}, True),
        ({"executor_name": "pbs", "url": None}, False),
        ({"executor_name": "slurm", "url": "remote://pbs"}, False),
    ],
)
def test_recovery_capability_is_explicit(options, expected):
    assert supports_recovery(options) is expected


def test_recover_live_submission_exposes_native_job_and_files(monkeypatch, tmp_path):
    calls = []
    def scheduler_recover(run_id, directory, *, submitted_at=None):
        calls.append((run_id, directory, submitted_at))
        return {
            "native_id": "456", "state": RunState.RUNNING,
            "return_code": None, "directory": directory,
            "stdout": str(tmp_path / "run.out"), "stderr": str(tmp_path / "run.err"),
        }
    monkeypatch.setattr(slurm, "recover", scheduler_recover)
    reply = PsijBackend.recover("run-456", {
        "directory": str(tmp_path), "backend_options": {"executor_name": "slurm"},
    })
    assert calls == [("run-456", str(tmp_path), None)]
    assert decode_handle(reply["remote_id"])["native_id"] == "456"
    assert reply["state"] == "running"
    assert reply["output_file"] == str(tmp_path / "run.out")
    assert reply["error_file"] == str(tmp_path / "run.err")


def test_recovery_without_reliable_scheduler_facts_is_unsupported(monkeypatch, tmp_path):
    monkeypatch.setattr(slurm, "recover",
                        lambda *a, **k: (_ for _ in ()).throw(NotImplementedError("accounting unavailable")))
    reply = PsijBackend.recover("run-id", {"directory": str(tmp_path)})
    assert reply["unsupported"] is True
    assert "accounting unavailable" in reply["error"]


def test_nonlocal_scheduler_cannot_use_local_slurm_recovery(monkeypatch, tmp_path):
    monkeypatch.setattr(slurm, "recover",
                        lambda *a, **k: pytest.fail("must not query local scheduler"))
    reply = PsijBackend.recover("run-id", {
        "directory": str(tmp_path),
        "backend_options": {"executor_name": "slurm", "url": "ssh://scheduler"},
    })
    assert reply["unsupported"] is True


def test_slurm_live_recovery(monkeypatch, tmp_path):
    calls = []
    def output(argv):
        calls.append(tuple(argv))
        if argv[0] == "squeue":
            return "123|" + slurm.submission_marker("run-1") + "|RUNNING|" + str(tmp_path) + "\n"
        if argv[:4] == ("scontrol", "--oneliner", "show", "job"):
            return "JobId=123 WorkDir=" + str(tmp_path) + " StdOut=" + str(tmp_path / "run.out") + " StdErr=" + str(tmp_path / "run.err")
        raise AssertionError(argv)
    monkeypatch.setattr(slurm, "_scheduler_output", output)
    facts = slurm.recover("run-1", str(tmp_path))
    assert facts["native_id"] == "123"
    assert facts["state"] == RunState.RUNNING
    assert facts["stdout"] == str(tmp_path / "run.out")
    assert calls[0][0] == "squeue"


@pytest.mark.parametrize("exit_code,expected", ((0, RunState.FINISHED), (7, RunState.FAILED)))
def test_slurm_accounting_recovers_terminal_status(monkeypatch, tmp_path, exit_code, expected):
    def output(argv):
        if argv[0] == "squeue":
            return ""
        if argv[:3] == ("scontrol", "show", "config"):
            return "AccountingStoreFlags = job_comment,job_env\n"
        if argv[0] == "sacct":
            return "456|" + slurm.submission_marker("run-2") + "|COMPLETED|" + str(tmp_path) + "|out|err|" + str(exit_code) + ":0|\n"
        raise AssertionError(argv)
    monkeypatch.setattr(slurm, "_scheduler_output", output)
    facts = slurm.recover("run-2", str(tmp_path), submitted_at="2026-10-13T08:00:00+02:00")
    assert facts["native_id"] == "456"
    assert facts["state"] == expected
    assert facts["return_code"] == exit_code


def test_slurm_accounting_absence_is_not_proof_of_no_submission(monkeypatch, tmp_path):
    def output(argv):
        if argv[0] == "squeue":
            return ""
        if argv[:3] == ("scontrol", "show", "config"):
            return "AccountingStoreFlags = job_comment\n"
        if argv[0] == "sacct":
            return ""
        raise AssertionError(argv)
    monkeypatch.setattr(slurm, "_scheduler_output", output)
    with pytest.raises(NotImplementedError, match="uncertain"):
        slurm.recover("run", str(tmp_path), submitted_at="2026-10-13T08:00:00+02:00")


def test_slurm_recovery_rejects_ambiguous_live_jobs(monkeypatch, tmp_path):
    marker = slurm.submission_marker("duplicate")
    monkeypatch.setattr(slurm, "_scheduler_output", lambda argv:
                        "123|" + marker + "|RUNNING|/one\n124|" + marker + "|PENDING|/two\n")
    with pytest.raises(RuntimeError, match="Multiple live Slurm jobs"):
        slurm.recover("duplicate", str(tmp_path))


def test_slurm_recovery_handles_job_disappearing_before_details(monkeypatch, tmp_path):
    marker = slurm.submission_marker("run-race")
    def output(argv):
        if argv[0] == "squeue":
            return "123|" + marker + "|RUNNING|" + str(tmp_path) + "\n"
        if argv[:4] == ("scontrol", "--oneliner", "show", "job"):
            raise subprocess.CalledProcessError(1, argv)
        if argv[:3] == ("scontrol", "show", "config"):
            return "AccountingStoreFlags = job_comment\n"
        if argv[0] == "sacct":
            return "123|" + marker + "|COMPLETED|" + str(tmp_path) + "|out|err|0:0|\n"
        raise AssertionError(argv)
    monkeypatch.setattr(slurm, "_scheduler_output", output)
    facts = slurm.recover("run-race", str(tmp_path),
                          submitted_at="2026-10-13T08:00:00+02:00")
    assert facts["state"] is RunState.FINISHED
    assert facts["return_code"] == 0


def test_slurm_status_nonzero_completion_is_failed(monkeypatch):
    monkeypatch.setattr(slurm, "_slurm_details", lambda native_id: {
        "JobState": "COMPLETED", "ExitCode": "7:0",
    })
    facts = slurm.status("123")
    assert facts.state is RunState.FAILED
    assert facts.return_code == 7
    assert facts.failure_phase is JobFailurePhase.EXECUTION


@pytest.mark.parametrize("output", [False, True])
def test_target_follow_reads_scheduler_files_only_on_request(monkeypatch, tmp_path, output):
    stdout = tmp_path / "stdout.txt"
    stdout.write_bytes(b"hello\n")
    native = NativeJob()
    native.update("COMPLETED", 0)
    monkeypatch.setattr("kkrunner.backends.implementations.psij.support._attach_job", lambda identity: native)
    out, err = io.BytesIO(), io.BytesIO()
    if not output:
        def unexpected(*args, **kwargs):
            pytest.fail("A status-only wait must not open a file tail")
        monkeypatch.setattr("kkrunner.backends.implementations.psij.support.follow_growing_files", unexpected)
    async def observe():
        return await PsijBackend.follow(
            handle(executor_name="pbs"), out, err, stdout_path=str(stdout),
            stderr_path=None, output=output, control=asyncio.StreamReader(),
        )
    facts = asyncio.run(observe())
    assert facts["output_drained"] is output
    assert out.getvalue() == (b"hello\n" if output else b"")
    assert err.getvalue() == b""
    assert facts["state"] == "finished"


def test_slurm_follow_uses_scheduler_facts_when_native_exit_code_is_missing(monkeypatch, tmp_path):
    """PSI/J supplies the notification; Slurm supplies the job facts."""
    stdout = tmp_path / "stdout.txt"
    stdout.write_bytes(b"hello\n")
    native = NativeJob()
    native.update("COMPLETED")
    monkeypatch.setattr("kkrunner.backends.implementations.psij.support._attach_job", lambda identity: native)
    queries = []

    def scheduler_status(native_id):
        queries.append(native_id)
        return JobStatus(RunState.FINISHED, return_code=0)

    monkeypatch.setattr(slurm, "status", scheduler_status)
    out, err = io.BytesIO(), io.BytesIO()
    facts = asyncio.run(PsijBackend.follow(
        handle("123"), out, err, stdout_path=str(stdout), stderr_path=None,
    ))
    assert queries == ["123"]
    assert facts["state"] == "finished"
    assert facts["return_code"] == 0
    assert out.getvalue() == b"hello\n"


def test_slurm_follow_prefers_authoritative_failure_to_native_success(monkeypatch, tmp_path):
    native = NativeJob()
    native.update("COMPLETED", 0)
    monkeypatch.setattr("kkrunner.backends.implementations.psij.support._attach_job", lambda identity: native)
    monkeypatch.setattr(
        slurm, "status",
        lambda native_id: JobStatus(RunState.FAILED, 7, JobFailurePhase.EXECUTION),
    )
    facts = asyncio.run(PsijBackend.follow(handle(), io.BytesIO(), io.BytesIO()))
    assert facts["state"] == "failed"
    assert facts["return_code"] == 7
    assert facts["failure_phase"] == "execution"


def test_slurm_follow_waits_for_authoritative_terminal_state(monkeypatch):
    """PSI/J completion may precede the scheduler's terminal record."""
    native = NativeJob()
    native.update("COMPLETED")
    monkeypatch.setattr("kkrunner.backends.implementations.psij.support._attach_job", lambda identity: native)
    states = iter((JobStatus(RunState.RUNNING), JobStatus(RunState.FINISHED, return_code=0)))
    queries = []

    def scheduler_status(native_id):
        queries.append(native_id)
        return next(states)

    monkeypatch.setattr(slurm, "status", scheduler_status)
    facts = asyncio.run(PsijBackend.follow(
        handle(), io.BytesIO(), io.BytesIO(), poll_interval=0.01,
    ))
    assert facts == {
        "state": "finished", "return_code": 0, "failure_phase": None, "error": None,
    }
    assert queries == ["123", "123"]


def test_slurm_status_uses_same_authority_as_follow(monkeypatch):
    queried = []

    def scheduler_status(native_id):
        queried.append(native_id)
        return JobStatus(RunState.FINISHED, return_code=0)

    monkeypatch.setattr(slurm, "status", scheduler_status)
    assert PsijBackend.status([handle()])[0] == {
        "state": "finished", "return_code": 0, "failure_phase": None, "error": None,
    }
    assert queried == ["123"]


def test_psij_backend_is_not_a_client_session():
    assert not hasattr(PsijBackend, "create_job")
    assert not hasattr(PsijBackend, "create_run_parts")


def test_slurm_batch_queries_only_requested_jobs_and_preserves_input_order(monkeypatch):
    commands = []
    def scheduler(argv):
        commands.append(argv)
        if argv[0] == "squeue":
            return "103|PENDING\n101|RUNNING\n999|RUNNING\n"
        assert argv[0] == "sacct"
        return "104|CANCELLED|0:15|\n102|COMPLETED|7:0|\n999|COMPLETED|0:0|\n"

    monkeypatch.setattr(slurm, "_scheduler_output", scheduler)
    monkeypatch.setattr(slurm, "_slurm_details", lambda _: pytest.fail("No per-job query needed"))
    monkeypatch.setattr("kkrunner.backends.implementations.psij.support._attach_job",
                        lambda _: pytest.fail("Slurm status must not attach PSI/J jobs"))
    refs = [handle(native_id) for native_id in ("101", "102", "103", "104", "101")]
    facts = PsijBackend.status(refs)
    assert [item["state"] for item in facts] == ["running", "failed", "queued", "cancelled", "running"]
    assert [item["return_code"] for item in facts] == [None, 7, None, 0, None]
    assert len(commands) == 2
    assert "--jobs=101,102,103,104" in commands[0]
    assert commands[1][commands[1].index("-j") + 1] == "102,104"


def test_slurm_batch_uses_controller_when_accounting_has_not_seen_job(monkeypatch):
    commands = []
    def scheduler(argv):
        commands.append(argv)
        if argv[0] == "squeue":
            return "101|RUNNING\n"
        return ""

    checked = []
    def details(native_id):
        checked.append(native_id)
        return {"JobState": "COMPLETED", "ExitCode": "0:0"} if native_id == "102" else {}

    monkeypatch.setattr(slurm, "_scheduler_output", scheduler)
    monkeypatch.setattr(slurm, "_slurm_details", details)
    facts = PsijBackend.status([handle("101"), handle("102"), handle("103")])
    assert [item["state"] for item in facts] == ["running", "finished", "unknown"]
    assert checked == ["102", "103"]
    assert len(commands) == 2


def test_non_slurm_batch_isolates_failed_native_observations(monkeypatch):
    def attach(ref):
        if ref["native_id"] == "broken":
            raise OSError("not reachable")
        native = NativeJob()
        native.update("ACTIVE")
        return native

    monkeypatch.setattr("kkrunner.backends.implementations.psij.support._attach_job", attach)
    facts = PsijBackend.status([handle(native_id, "pbs") for native_id in ("first", "broken", "last")])
    assert [item["state"] for item in facts] == ["running", "unknown", "running"]
    assert "not reachable" in facts[1]["error"]


def test_psij_subscription_changes_do_not_attach_to_scheduler_again(monkeypatch, tmp_path):
    from kkrunner.backends.implementations.psij import support

    async def exercise():
        native = NativeJob()
        native.update("ACTIVE")
        attachments = []
        attached = asyncio.Event()
        stopped = asyncio.Event()
        received = asyncio.Event()
        stdout = tmp_path / "stdout"
        stdout.write_bytes(b"first\n")
        control = asyncio.StreamReader()
        original = support.follow_growing_files

        def attach(identity):
            attachments.append(identity)
            attached.set()
            return native

        async def tail(*args, **kwargs):
            try:
                return await original(*args, **kwargs)
            finally:
                stopped.set()

        class Sink(io.BytesIO):
            def write(self, data):
                count = super().write(data)
                received.set()
                return count

        monkeypatch.setattr(support, "_attach_job", attach)
        monkeypatch.setattr(support, "follow_growing_files", tail)
        out = Sink()
        task = asyncio.create_task(PsijBackend.follow(
            handle(executor_name="pbs"), out, io.BytesIO(), stdout_path=str(stdout),
            output=False, control=control,
        ))
        try:
            await asyncio.wait_for(attached.wait(), 2)
            assert out.getvalue() == b""
            control.feed_data(b"follow\n")
            await asyncio.wait_for(received.wait(), 2)
            assert out.getvalue() == b"first\n"
            control.feed_data(b"unfollow\n")
            await asyncio.wait_for(stopped.wait(), 2)
            stdout.write_bytes(b"first\nsecond\n")
            native.update("COMPLETED", 0)
            facts = await asyncio.wait_for(task, 2)
            assert facts["return_code"] == 0
            assert facts["output_drained"] is False
            assert out.getvalue() == b"first\n"
            assert len(attachments) == 1
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(exercise())



def test_failed_optional_psij_tail_can_be_restarted_without_reattaching(monkeypatch, tmp_path):
    from kkrunner.backends.implementations.psij import support

    async def check():
        native = NativeJob()
        native.update("ACTIVE")
        failed = asyncio.Event()
        received = asyncio.Event()
        attempts = []
        attachments = []
        original = support.follow_growing_files
        path = tmp_path / "out"
        path.write_bytes(b"output")

        def attach(identity):
            attachments.append(identity)
            return native

        async def tail(*args, **kwargs):
            attempts.append(True)
            if len(attempts) == 1:
                failed.set()
                raise PermissionError("cannot read output")
            return await original(*args, **kwargs)

        class Sink(io.BytesIO):
            def write(self, data):
                count = super().write(data)
                received.set()
                return count

        monkeypatch.setattr(support, "_attach_job", attach)
        monkeypatch.setattr(support, "follow_growing_files", tail)
        control = asyncio.StreamReader()
        out = Sink()
        task = asyncio.create_task(PsijBackend.follow(
            handle(executor_name="pbs"), out, io.BytesIO(), stdout_path=str(path), control=control,
        ))
        try:
            await asyncio.wait_for(failed.wait(), 2)
            assert not task.done()
            control.feed_data(b"unfollow\nfollow\n")
            await asyncio.wait_for(received.wait(), 2)
            native.update("COMPLETED", 0)
            facts = await asyncio.wait_for(task, 2)
            assert facts["state"] == "finished"
            assert facts["return_code"] == 0
            assert facts["output_error"] == "cannot read output"
            assert out.getvalue() == b"output"
            assert len(attachments) == 1
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(check())
