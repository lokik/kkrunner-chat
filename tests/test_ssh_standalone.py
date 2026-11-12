"""SSH target contains only KKRunner, PSI/J and Python, without ASE."""

import os
import subprocess
import time
from threading import Event

import pytest

from kkrunner import ExecutionSpec, RunState, RunStore
from kkrunner.client import StagingSpec, EndpointClient
from kkrunner.client import SshTransport
from kkrunner.backends import SupervisedBackend
from kkrunner.runs.manager import RunManager

pytestmark = pytest.mark.skipif(
    os.environ.get("KKRUNNER_RUN_DOCKER_TESTS") != "1", reason="set KKRUNNER_RUN_DOCKER_TESTS=1 for SSH integration"
)


@pytest.mark.parametrize("detach_after_output", [False, True])
def test_ssh_execution_detach_and_reconnect_without_application(docker_execution_image, tmp_path, detach_after_output):
    image = docker_execution_image
    key = tmp_path / "test-key"
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(key)], check=True)
    container = subprocess.check_output(
        [
            image.docker,
            "create",
            "-p",
            "127.0.0.1::22",
            "-e",
            "KKRUNNER_TEST_SERVICES=ssh",
            image.image,
            "sleep",
            "180",
        ],
        text=True,
    ).strip()
    try:
        subprocess.run([image.docker, "cp", str(key) + ".pub", container + ":/authorized_keys"], check=True)
        subprocess.run([image.docker, "start", container], check=True, stdout=subprocess.DEVNULL)
        port = subprocess.check_output([image.docker, "port", container, "22/tcp"], text=True).strip().rsplit(":", 1)[1]
        options = (
            "-p",
            port,
            "-i",
            str(key),
            "-o",
            "IdentitiesOnly=yes",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
        )
        command = ["ssh", *options, "tester@127.0.0.1", "true"]
        for _ in range(100):
            if subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
                break
            time.sleep(0.1)
        else:
            pytest.fail("SSH service did not become ready")
        # This assertion executes in the target's interpreter, not the client.
        subprocess.run(
            [
                "ssh",
                *options,
                "tester@127.0.0.1",
                "/opt/venv/bin/python",
                "-c",
                "\"import importlib.util; assert importlib.util.find_spec('ase') is None; "
                "assert importlib.util.find_spec('ase2sprkkr') is None; assert importlib.util.find_spec('kkrunner') is not None\"",
            ],
            check=True,
        )
        local = tmp_path / "work"
        local.mkdir()
        manager = RunManager(RunStore(tmp_path / "runs"))
        executor = EndpointClient(
            SupervisedBackend,
            transport=SshTransport(
                "tester@127.0.0.1", options=options,
                python_command=("/opt/venv/bin/python",),
            ),
        )
        spec = ExecutionSpec(
            [
                "/opt/venv/bin/python",
                "-c",
                # The job cannot finish before the second client attaches.
                # Timing-only sleeps made the test race with a fast observer.
                "from pathlib import Path\n"
                "import time\n"
                "print('started', flush=True)\n"
                "deadline = time.monotonic() + 60\n"
                "while not Path('release.txt').exists():\n"
                "    if time.monotonic() >= deadline:\n"
                "        raise RuntimeError('test never released the job')\n"
                "    time.sleep(0.05)\n"
                "Path('answer.txt').write_text('42')\n"
                "print('done')\n",
            ],
            directory=local,
            output_file=local / "stdout.txt",
            output_files=(local / "answer.txt",),
            staging=StagingSpec(local_root=local, remote_root="/home/tester/jobs"),
        )
        run = manager.create_run(executor, spec)
        first_output = Event()
        run.add_output_listener(lambda text, kind: first_output.set() if "started" in text else None)
        run.submit()
        if detach_after_output:
            assert first_output.wait(10)
        backend_id = run.backend_id
        run.stop_observation()
        run.wait()
        # The job is deliberately blocked until attachment is established.
        # A terminal RunRecord cannot be reattached, by design.
        record = manager.store.get(run.id)
        assert not record.state.is_terminal, record
        other = RunManager(RunStore(manager.store.directory))
        try:
            attached = other.attach(run.id)
        finally:
            # Also release the waiting job if attachment raises unexpectedly.
            subprocess.run(
                ["ssh", *options, "tester@127.0.0.1", "touch /home/tester/jobs/release.txt"],
                check=True,
            )
        outcome = attached.wait()
        assert outcome.return_code == 0, (attached.state, attached.failure_message, other.store.get(run.id))
        assert attached.backend_id == backend_id
        assert other.store.get(run.id).state == RunState.FINISHED
        assert (local / "answer.txt").read_text() == "42"
        assert (local / "stdout.txt").read_text().splitlines() == ["started", "done"]
    finally:
        subprocess.run([image.docker, "rm", "-f", container], check=True, stdout=subprocess.DEVNULL)
