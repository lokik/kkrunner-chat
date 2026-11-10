"""Byte-output decoding and delivery for local, remote and PSI/J execution."""

import asyncio
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from kkrunner.backends.process.engine import ProcessJob
from kkrunner.backends.process.managed import _ObserverSink
from kkrunner.backends.scheduler.engine import PsijJob
from kkrunner.client.remote_job import RemoteJob
from kkrunner.runs.base import OutputObservers


@pytest.mark.parametrize(
    "data, expected",
    [
        (b"", []),
        (b"ASCII", [("ASCII", "err")]),
        ("Příliš".encode("utf8"), [("Příliš", "err")]),
        (b"broken:\xff", [("broken:�", "err")]),
    ],
)
def test_emit_bytes_decodes_nonempty_chunks(data, expected):
    received = []
    observers = OutputObservers(lambda text, kind: received.append((text, kind)))
    observers.emit_bytes(data, "err")
    assert received == expected


@pytest.mark.parametrize("kind", ("out", "err"))
def test_local_forward_output_persists_and_observes_bytes(kind):
    received = []
    output = BytesIO()
    runner = object.__new__(ProcessJob)
    runner.spec = SimpleNamespace(output_file=output, error_file=None)
    runner._output_observers = OutputObservers(lambda text, stream: received.append((text, stream)))
    runner._forward_output(b"message:\xff", kind)
    runner._forward_output(b"", kind)
    assert output.getvalue() == (b"message:\xff" if kind == "out" else b"")
    assert received == [("message:�", kind)]


def test_remote_pump_delivers_bytes_without_runner_decoder():
    received, written = ([], [])
    runner = object.__new__(RemoteJob)
    runner._output_observers = OutputObservers(lambda text, kind: received.append((text, kind)))
    runner._stream_writer = lambda data, kind: written.append((data, kind))
    source = SimpleNamespace(read=AsyncMock(side_effect=[b"remote:\xff", b""]))
    asyncio.run(runner._pump(source, "out"))
    assert written == [(b"remote:\xff", "out")]
    assert received == [("remote:�", "out")]


def test_psij_monitor_forwards_follow_bytes(monkeypatch):
    import kkrunner.backends.scheduler.engine as psij_module

    received = []

    async def follow(paths, on_chunk, terminal, poll_interval):
        assert paths == {"out": "job.out", "err": "job.err"}
        on_chunk("out", "výstup".encode("utf8"))
        on_chunk("err", b"warning:\xff")
        return SimpleNamespace(exit_code=0, state=SimpleNamespace(name="COMPLETED"))

    monkeypatch.setattr(psij_module, "follow_growing_files", follow)
    runner = object.__new__(PsijJob)
    runner._stdout_path = "job.out"
    runner._stderr_path = "job.err"
    runner._job = SimpleNamespace(status=SimpleNamespace(state=SimpleNamespace(name="COMPLETED"), exit_code=0))
    runner.executor = SimpleNamespace(poll_interval=1.0, _observe_job=lambda job, observer: lambda: None)
    runner._output_observers = OutputObservers(lambda text, kind: received.append((text, kind)))
    runner.result = SimpleNamespace(return_code=None)
    runner._files = None
    runner._observed_state = None
    from threading import Event, Lock

    runner._observation_stopped = Event()
    runner._observation_signal_lock = Lock()
    runner._observation_wakeup = None
    result = asyncio.run(runner._monitor())
    assert result.return_code == 0
    assert received == [("výstup", "out"), ("warning:�", "err")]


def test_local_project_sink_uses_shared_bytes_policy():
    received = []
    sink = _ObserverSink(OutputObservers(lambda text, kind: received.append((text, kind))), "out")
    sink.write("výstup".encode("utf8"))
    sink.write(b"")
    sink.flush()
    assert received == [("výstup", "out")]


def test_observer_failure_does_not_prevent_other_byte_listeners():
    received, failures = ([], [])
    observers = OutputObservers()

    def broken(text, kind):
        raise RuntimeError("listener failed")

    observers.add(broken)
    observers.add(lambda text, kind: received.append((text, kind)))
    observers.add_error_listener(lambda callback, exc: failures.append((callback, str(exc))))
    observers.emit_bytes(b"first", "out")
    observers.emit_bytes(b"second", "out")
    assert received == [("first", "out"), ("second", "out")]
    assert failures == [(broken, "listener failed")]
