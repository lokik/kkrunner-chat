from dataclasses import replace
from types import SimpleNamespace

from kkrunner.backends import ProcessBackend, PsijBackend
from kkrunner.backends.contracts import JobAttachment
from kkrunner.backends.scheduler.engine import PsijJob
from kkrunner.client.endpoint import ExecutionEndpoint
from kkrunner.client.run_parts import RunParts
from kkrunner.files.manifest import FileEntry, FileManifest, FileMapping
from kkrunner.client.agent import _AgentRunContext
from kkrunner.client.remote_job import RemoteJob
from kkrunner.runs.base import LaunchSpec, RunState
from kkrunner.runs.manager import RunManager
from kkrunner.runs.model import Execution, WorkingDirectory
from kkrunner.runs.records import RunRecord, RunStore


class _AttachedRunner:
    def __init__(self, run):
        self.backend_id = run.execution.backend_id
        self.result = SimpleNamespace(return_code=0)
        self.state = RunState.RUNNING
        self.exception = None
        self.failure_phase = None
        self.failure_message = None

    def add_output_listener(self, callback):
        pass

    def add_output_error_listener(self, callback):
        pass

    def start(self):
        self.state = RunState.FINISHED

    def wait(self):
        return self.result

    def cancel(self):
        pass


class _AttachExecutor(ProcessBackend):
    def acknowledge(self, run):
        pass

    def open_run_parts(self, attachment, *, callback=None, output_listener=None):
        if output_listener is not None:
            output_listener("manager replay\\n", "out")
        return RunParts(_AttachedRunner(attachment))


def test_manager_attach_forwards_initial_output_listener(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(directory=tmp_path, metadata={"test": {"task": "SCF"}}, processing_required=True)
    record = replace(record, state=RunState.RUNNING, execution=record.execution.submitted("123"))
    manager.store.save(record)
    output = []
    runner = manager.attach(
        record,
        backend=_AttachExecutor(),
        process_result=lambda result: result,
        output_listener=lambda text, kind: output.append((text, kind)),
    )
    runner.wait()
    assert output == [("manager replay\n", "out")]


def _remote_context(tmp_path):

    class Endpoint:
        backend = "process"
        backend_options = {}
        command = ("kkrunner",)
        computer = "host"
        transport = SimpleNamespace()

        async def _remote_home(self):
            return "/remote"

        async def _status(self, directory, run_id, files=None):
            return {
                "state": "running",
                "input_file": "/remote/work/input.inp",
                "output_file": "/remote/work/output.out",
            }

    context = _AgentRunContext(Endpoint(), LaunchSpec(local_root=str(tmp_path), remote_root="/remote"))
    run = RunRecord(
        id="run",
        state=RunState.RUNNING,
        created_at="2026-10-13T08:00:00+00:00",
        updated_at="2026-10-13T08:00:00+00:00",
        execution=Execution(
            ExecutionEndpoint(executor="ssh", host="host"),
            WorkingDirectory(str(tmp_path / "work"), "/remote/work"),
            "run",
        ),
        files=FileManifest(
            stdout=FileEntry(local_path=str(tmp_path / "work" / "output.out"), remote_path="/remote/work/output.out"),
            mapping=FileMapping(str(tmp_path), "/remote"),
        ),
        metadata={"test": {"task": "SCF"}},
        processing_required=True,
    )
    return (context, run)


def test_remote_attach_installs_listener_before_monitor(monkeypatch, tmp_path):
    events = []

    def immediate_monitor(self):
        self._output_observers.emit_bytes(b"remote replay\n", "out")
        if self.callback is not None:
            self.callback(self.result)
        self._done.set()

    monkeypatch.setattr(RemoteJob, "_thread_main", immediate_monitor)
    context, run = _remote_context(tmp_path)
    import asyncio

    parts = asyncio.run(context.attach_run_parts(
        JobAttachment(run.execution, run.files),
        callback=lambda result: events.append(("finished", None)),
        output_listener=lambda text, kind: events.append((text, kind)),
    ))
    runner = parts.job
    runner.start()
    runner._done.wait(1)
    parts.files.close()
    assert events == [("remote replay\n", "out"), ("finished", None)]


def test_remote_attach_can_defer_monitor_start(monkeypatch, tmp_path):
    events = []

    def immediate_monitor(self):
        self._output_observers.emit_bytes(b"remote replay\n", "out")
        self._done.set()

    monkeypatch.setattr(RemoteJob, "_thread_main", immediate_monitor)
    context, run = _remote_context(tmp_path)
    import asyncio

    parts = asyncio.run(context.attach_run_parts(
        JobAttachment(run.execution, run.files),
        output_listener=lambda text, kind: events.append((text, kind)),
    ))
    runner = parts.job
    assert runner._thread is None
    assert events == []
    runner.start()
    runner._done.wait(1)
    parts.files.close()
    assert events == [("remote replay\n", "out")]


def _psij_executor():

    class Job:
        def __init__(self):
            self.native_id = "42"
            self.status = SimpleNamespace(state=SimpleNamespace(name="ACTIVE"))

    class JobExecutor:
        def attach(self, job, native_id):
            job.native_id = native_id

    executor = object.__new__(PsijBackend)
    executor.backend = "slurm"
    executor.url = None
    executor.poll_interval = 0.01
    executor._psij = SimpleNamespace(Job=Job)
    executor._job_executor = JobExecutor()
    from collections import OrderedDict
    from threading import RLock

    executor._jobs = OrderedDict()
    executor._observers = {}
    executor._jobs_lock = RLock()
    executor._max_idle_jobs = 64
    return executor


def _psij_run(tmp_path):
    return RunRecord(
        id="psij-run",
        state=RunState.RUNNING,
        created_at="2026-10-13T08:00:00+00:00",
        updated_at="2026-10-13T08:00:00+00:00",
        execution=Execution(
            ExecutionEndpoint(executor="psij", psij_backend="slurm"), WorkingDirectory(str(tmp_path)), "42"
        ),
        files=FileManifest(
            stdout=FileEntry(local_path=str(tmp_path / "output.out")),
            stderr=FileEntry(local_path=str(tmp_path / "error.err")),
        ),
        metadata={"test": {"task": "SCF"}},
        processing_required=True,
    )


def test_psij_attach_installs_listener_before_monitor(monkeypatch, tmp_path):
    events = []

    def immediate_monitor(self):
        self._output_observers.emit_bytes(b"psij replay\n", "out")
        if self.callback is not None:
            self.callback(self.result)
        self._done.set()

    monkeypatch.setattr(PsijJob, "_thread_main", immediate_monitor)
    runner = _psij_executor().open_job(
        JobAttachment(_psij_run(tmp_path).execution, _psij_run(tmp_path).files),
        callback=lambda result: events.append(("finished", None)),
        output_listener=lambda text, kind: events.append((text, kind)),
    )
    runner._done.wait(1)
    assert events == [("psij replay\n", "out"), ("finished", None)]


def test_psij_attach_can_defer_monitor_start(monkeypatch, tmp_path):
    events = []

    def immediate_monitor(self):
        self._output_observers.emit_bytes(b"psij replay\n", "out")
        self._done.set()

    monkeypatch.setattr(PsijJob, "_thread_main", immediate_monitor)
    runner = _psij_executor().open_job(
        JobAttachment(_psij_run(tmp_path).execution, _psij_run(tmp_path).files),
        output_listener=lambda text, kind: events.append((text, kind)), start=False,
    )
    assert runner._thread is None
    assert events == []
    runner.start()
    runner._done.wait(1)
    assert events == [("psij replay\n", "out")]
