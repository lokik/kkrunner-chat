import importlib.metadata as md
import importlib.util
import json
import os
import pkgutil
import subprocess
import sys
import tempfile
from pathlib import Path

import kkrunner
from kkrunner import ExecutionSpec, ProcessingPending, Run, RunState
from kkrunner.backends import DirectBackend, SupervisedBackend
from kkrunner.client import EndpointClient, SshTransport, StagingSpec
from kkrunner.projects import ProjectRunManager, ProjectRepository

assert not os.environ.get("PYTHONPATH")
assert importlib.util.find_spec("ase") is None
assert importlib.util.find_spec("ase2sprkkr") is None
for module in pkgutil.walk_packages(kkrunner.__path__, kkrunner.__name__ + "."):
    if not module.name.endswith("__main__"):
        __import__(module.name)
assert md.version("kkrunner") == kkrunner.__version__

root = Path(tempfile.mkdtemp(prefix="kkrunner-distribution-"))
raw = Run.untracked(
    EndpointClient(DirectBackend),
    ExecutionSpec(
        [sys.executable, "-c", "print(42)"],
        directory=root,
        output_file="raw.out",
    )
)
assert raw.run().return_code == 0
assert (root / "raw.out").read_text() == "42\n"

projects = ProjectRepository(root / "projects")
project = projects.create("Wheel review", directory=root)
manager = ProjectRunManager(project_repository=projects, project=project)
run = manager.create_run(
    EndpointClient(SupervisedBackend),
    ExecutionSpec(
        [sys.executable, "-c", "print(43)"],
        directory=root,
        output_file="managed.out",
    ),
    processing_required=True,
)
try:
    run.run()
except ProcessingPending:
    pass
assert manager.store.get(run.id).state == RunState.POSTPROCESSING
restarted = ProjectRunManager(project_repository=projects, project=project)
attached = restarted.attach(run.id, process_result=lambda outcome: int(Path(outcome.output_file).read_text()))
assert attached.wait() == 43
assert restarted.store.get(run.id).state == RunState.FINISHED

local, remote = root / "local", root / "remote"
local.mkdir()
remote.mkdir()
class LoopbackSshTransport(SshTransport):
    """Run SSH target commands locally using the real operation protocol."""

    def __init__(self):
        super().__init__("loopback", python_command=(sys.executable,))

    def _command(self, argv, cwd=None):
        assert cwd is None
        return tuple(argv)


client = EndpointClient(DirectBackend, transport=LoopbackSshTransport())
run = Run.untracked(client,
    ExecutionSpec(
        [sys.executable, "-c", "print(44)"],
        directory=local,
        output_file="remote.out",
        staging=StagingSpec(local_root=local, remote_root=remote),
    )
)
assert run.run().return_code == 0
assert (local / "remote.out").read_text() == "44\n"
commands = {entry.name for entry in md.distribution("kkrunner").entry_points if entry.group == "console_scripts"}
assert commands == {"kkrunner", "kkrunner-agent"}, commands
assert importlib.util.find_spec("kkrunner_cli") is not None
for command in ("kkrunner", "kkrunner-agent"):
    for arguments in (("--help",), ("status", "--help")):
        subprocess.run([str(Path(sys.executable).parent / command), *arguments], check=True, stdout=subprocess.DEVNULL)
print(
    json.dumps(
        {
            "distribution": md.version("kkrunner"),
            "module": kkrunner.__file__,
            "raw": "ok",
            "managed_reconnect": "ok",
            "ssh_loopback_execution": "ok",
            "without_ase": True,
        }
    )
)
