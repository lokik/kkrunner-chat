"""SSH target contains only KKRunner, PSI/J and Python, without ASE."""

import os
import subprocess
import time
from threading import Event

import pytest

from kkrunner import ExecutionSpec, RunState, RunStore
from kkrunner.runs import LaunchSpec
from kkrunner.client import SshRunEndpoint, SshTransport
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
                "assert importlib.util.find_spec('ase2sprkkr') is None\"",
            ],
            check=True,
        )
        local = tmp_path / "work"
        local.mkdir()
        manager = RunManager(RunStore(tmp_path / "runs"))
        executor = SshRunEndpoint(SshTransport("tester@127.0.0.1", options=options))
        spec = ExecutionSpec(
            [
                "/opt/venv/bin/python",
                "-c",
                "import time; from pathlib import Path; print('started',flush=True); time.sleep(2); Path('answer.txt').write_text('42'); print('done')",
            ],
            directory=local,
            output_file=local / "stdout.txt",
            output_files=(local / "answer.txt",),
            launch=LaunchSpec(local_root=local, remote_root="/home/tester/jobs"),
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
        other = RunManager(RunStore(manager.store.directory))
        attached = other.attach(run.id)
        outcome = attached.wait()
        assert outcome.return_code == 0, (attached.state, attached.failure_message, other.store.get(run.id))
        assert attached.backend_id == backend_id
        assert other.store.get(run.id).state == RunState.FINISHED
        assert (local / "answer.txt").read_text() == "42"
        assert (local / "stdout.txt").read_text().splitlines() == ["started", "done"]
    finally:
        subprocess.run([image.docker, "rm", "-f", container], check=True, stdout=subprocess.DEVNULL)
