"""Byte-output decoding and delivery for local, remote and PSI/J execution."""


import pytest

from kkrunner.client.job import Job
from kkrunner.client.backend_execution import DirectExecution
from kkrunner.targets import LocalTarget
from kkrunner.backends import DirectBackend
from kkrunner.runs.base import ExecutionSpec
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
def test_observed_output_delivers_decoded_bytes(kind):
    received = []
    job = Job(DirectExecution(DirectBackend(), LocalTarget()), ExecutionSpec(("true",)))
    job.add_output_listener(lambda text, stream: received.append((text, stream)))
    job._observed_output(b"message:\xff", kind)
    job._observed_output(b"", kind)
    assert received == [("message:�", kind)]


def test_observed_output_without_file_sink_decodes_bytes():
    received = []
    job = Job(DirectExecution(DirectBackend(), LocalTarget()), ExecutionSpec(("true",)))
    job.add_output_listener(lambda text, kind: received.append((text, kind)))
    job._observed_output("výstup".encode("utf8"), "out")
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


def test_removing_output_listener_does_not_remove_other_listeners():
    received = []
    observers = OutputObservers()

    def removed(text, kind):
        pytest.fail("Removed listener was called")

    def kept(text, kind):
        received.append((text, kind))

    observers.add(removed)
    observers.add(kept)
    observers.remove(removed)
    observers.remove(removed)
    observers.emit_bytes(b"first", "out")
    assert received == [("first", "out")]
    assert observers.has_listeners
    observers.remove(kept)
    assert not observers.has_listeners
    observers.emit_bytes(b"second", "out")
    assert received == [("first", "out")]
