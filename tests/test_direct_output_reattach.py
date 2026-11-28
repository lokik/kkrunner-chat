"""Direct attachments follow saved output without re-submitting a computation."""

import asyncio
import subprocess
import sys

import pytest

from kkrunner.backends import DirectBackend
from kkrunner.backends.config import BackendConfig
from kkrunner.client import ExecutionClient, LocalTarget, SshTarget
from kkrunner.client.contracts import JobAttachment, Operation
from kkrunner.files import FileEntry, FileManifest
from kkrunner.runs import ExecutionMetadata, WorkingDirectory


class LoopbackSshTransport(SshTarget):
    def __init__(self):
        super().__init__("loopback", python_command=(sys.executable,))

    def _command(self, argv, cwd=None):
        assert cwd is None
        return tuple(argv)


def linux_identity(pid):
    with open(f"/proc/{pid}/stat", encoding="ascii") as source:
        ticks = int(source.read().rsplit(")", 1)[1].split()[19])
    with open("/proc/sys/kernel/random/boot_id", encoding="ascii") as source:
        boot = source.read().strip()
    return f"direct:{pid}:{ticks}:{boot}"


def test_direct_output_attachment_does_not_submit(tmp_path):
    output = tmp_path / "output.txt"
    output.write_text("existing")
    endpoint = ExecutionClient(DirectBackend)
    attachment = JobAttachment(
        ExecutionMetadata(
            LocalTarget(), WorkingDirectory(str(tmp_path)),
            backend=BackendConfig("direct"), backend_id="legacy-opaque-id",
        ),
        FileManifest(stdout=FileEntry(local_path=str(output))),
    )
    parts = endpoint.open_run_parts(attachment)
    try:
        assert parts.job.backend_id == "legacy-opaque-id"
        assert parts.job.spec.argv is None
        assert parts.files.spec.output_file == str(output)
        parts.configure_observation()
        status = asyncio.run(asyncio.wait_for(parts.job.wait_async(), timeout=3))
        assert status.state.value == "unknown"
        assert "identity" in status.message
    finally:
        parts.close()


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux process identity")
@pytest.mark.parametrize("remote", [False, True])
@pytest.mark.parametrize("offset", [0, 6])
def test_direct_follow_local_and_portable_ssh(tmp_path, remote, offset):
    output = tmp_path / "stdout.txt"
    script = (
        "import time; from pathlib import Path; "
        f"out=Path({str(output)!r}).open('wb'); "
        "out.write(b'first\\n'); out.flush(); "
        "time.sleep(0.3); out.write(b'second\\n'); out.close()"
    )
    child = subprocess.Popen((sys.executable, "-c", script), start_new_session=True)
    try:
        token = linux_identity(child.pid)
        transport = LoopbackSshTransport() if remote else LocalTarget()
        received = []

        async def observe():
            return await asyncio.wait_for(transport.follow(
                Operation.for_backend(DirectBackend, "follow"), token,
                lambda data, kind: received.append((kind, data)),
                stdout_path=str(output), stderr_path=None,
                stdout_offset=offset, stderr_offset=None, poll_interval=0.05,
            ), timeout=5)

        facts = asyncio.run(observe())
        assert facts["state"] == "finished"
        assert facts["return_code"] is None
        assert {kind for kind, _ in received} == {"out"}
        expected = b"first\nsecond\n"[offset:]
        assert b"".join(chunk for _, chunk in received) == expected
    finally:
        if child.poll() is None:
            child.terminate()
        child.wait(timeout=5)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux process identity")
@pytest.mark.parametrize("remote", [False, True])
def test_stopping_direct_follow_does_not_cancel_calculation(tmp_path, remote):
    output = tmp_path / "stdout.txt"
    child = subprocess.Popen(
        (sys.executable, "-c", "import time; time.sleep(30)"),
        start_new_session=True,
    )
    try:
        token = linux_identity(child.pid)
        transport = LoopbackSshTransport() if remote else LocalTarget()

        async def observe():
            task = asyncio.create_task(transport.follow(
                Operation.for_backend(DirectBackend, "follow"), token,
                lambda data, kind: None, stdout_path=str(output),
                stderr_path=None, stderr_offset=None, poll_interval=0.05,
            ))
            await asyncio.sleep(0.2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        asyncio.run(observe())
        assert child.poll() is None
    finally:
        child.terminate()
        child.wait(timeout=5)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux process identity")
def test_direct_follow_waits_for_process_group(tmp_path):
    output = tmp_path / "stdout.txt"
    child_code = (
        "import time; from pathlib import Path; time.sleep(0.3); "
        f"Path({str(output)!r}).write_bytes(b'late output')"
    )
    leader_code = f"import subprocess, sys; subprocess.Popen([sys.executable, '-c', {child_code!r}])"
    leader = subprocess.Popen((sys.executable, "-c", leader_code), start_new_session=True)
    try:
        token = linux_identity(leader.pid)
        received = []
        result = asyncio.run(asyncio.wait_for(LocalTarget().follow(
            Operation.for_backend(DirectBackend, "follow"), token,
            lambda data, kind: received.append(data),
            stdout_path=str(output), stderr_offset=None, poll_interval=0.05,
        ), timeout=5))
        assert result["state"] == "finished"
        assert b"".join(received) == b"late output"
    finally:
        leader.wait(timeout=5)



@pytest.mark.parametrize("remote", [False, True])
def test_unverifiable_direct_identity_ends_observation(tmp_path, remote):
    transport = LoopbackSshTransport() if remote else LocalTarget()
    result = asyncio.run(asyncio.wait_for(transport.follow(
        Operation.for_backend(DirectBackend, "follow"), "legacy-opaque-id",
        lambda *_: None, stdout_path=str(tmp_path / "out"), poll_interval=0.05,
    ), timeout=3))
    assert result["state"] == "unknown"
    assert result["return_code"] is None
    assert "identity" in result["error"]
