"""Explicit Docker/Slurm integration test for the PSI/J executor."""

import os
import subprocess

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("KKRUNNER_RUN_DOCKER_TESTS") != "1",
    reason="set KKRUNNER_RUN_DOCKER_TESTS=1 to run Docker integration tests",
)


def test_psij_slurm_docker(docker_execution_image):
    docker = docker_execution_image.docker
    image = docker_execution_image.image
    subprocess.run(
        [
            docker,
            "run",
            "--rm",
            "--privileged",
            "--cgroupns=private",
            "--hostname",
            "node1",
            "-e",
            "KKRUNNER_TEST_SERVICES=slurm",
            image,
            "/opt/venv/bin/python",
            "/workspace/kkrunner/tests/docker/psij_slurm_test_in_docker.py",
        ],
        check=True,
    )
