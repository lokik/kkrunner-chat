"""Keep process-local client pools isolated between tests."""

import pytest

from kkrunner.client import pool


@pytest.fixture(autouse=True)
def isolated_default_client_pool():
    previous = pool._DEFAULT_CLIENT_POOL
    pool._DEFAULT_CLIENT_POOL = pool.ClientPool()
    try:
        yield
    finally:
        pool._DEFAULT_CLIENT_POOL.close()
        pool._DEFAULT_CLIENT_POOL = previous


@pytest.fixture(scope="session")
def docker_execution_image():
    import shutil
    import subprocess
    from pathlib import Path
    from types import SimpleNamespace

    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("Docker is required for scheduler integration")
    root = Path(__file__).resolve().parents[1]
    name = "kkrunner-execution-test"
    subprocess.run([docker, "build", "-f", str(root / "tests/docker/Dockerfile"), "-t", name, str(root)], check=True)
    return SimpleNamespace(docker=docker, repository=root, image=name)
