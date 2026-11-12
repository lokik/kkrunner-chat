"""Scheduler workload intentionally using no scientific application package."""

import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from kkrunner import ExecutionSpec, RunState, RunStore
from kkrunner.backends import PsijBackend
from kkrunner.client import EndpointClient
from kkrunner.runs.manager import RunManager


def main():
    with TemporaryDirectory(prefix="kkrunner-slurm-") as work:
        directory = Path(work)
        manager = RunManager(RunStore(directory / "runs"))
        run = manager.create_run(
            EndpointClient(PsijBackend, backend_options={"executor_name": "slurm"}),
            ExecutionSpec(
                [sys.executable, "-c", "from pathlib import Path; Path('result.txt').write_text('42'); print('done')"],
                directory=work,
                output_file=directory / "stdout.txt",
                output_files=(directory / "result.txt",),
            ),
        )
        outcome = run.run()
        assert outcome.return_code == 0, (
            f"Run: {run.state.value}, Job: {run._job.status()!r}, "
            f"message: {run.failure_message!r}, "
            f"output error: {run._job.output_error!r}, "
            f"record: {manager.store.get(run.id)!r}"
        )
        assert manager.store.get(run.id).state == RunState.FINISHED
        assert (directory / "result.txt").read_text() == "42"
        assert (directory / "stdout.txt").read_text().strip() == "done"
    print("Standalone KKRunner -> PSI/J -> Slurm passed")


if __name__ == "__main__":
    main()
