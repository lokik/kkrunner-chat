# KKRunner

**One Python API for running programs on your laptop, a remote machine or a
computing cluster.**

Run ordinary commands or MPI applications, launch jobs that survive the exit
of your Python session, reconnect to running jobs, transfer files over SSH
and keep a history of your calculations.

## Run a command

```python
from kkrunner import run

result = run(["echo", "Hello"])
print(result.return_code)
```

`run()` waits for the command to finish. Output is displayed live, or you can
save it with `output_file="run.out"`.

## Start a job and reconnect later

```python
from kkrunner import submit

job = submit(
    ["sh", "-c", "echo started; sleep 120; echo done"],
    backend="kkrunner",
    output_file="run.out",
    tracked=True,
)
print(job.id)
```

`submit()` starts the job without waiting for it to finish. The `kkrunner`
backend keeps it running even after you exit Python. `tracked=True` saves
its details so you can find it again by ID.

Exit Python, then reconnect from another Python session while the job runs:

```python
from kkrunner import RunManager

job = RunManager.get_default().attach("RUN_ID")
result = job.wait()
```

Replace `RUN_ID` with the printed ID. `job.wait()` waits for the result.
You can also monitor jobs from the command line (see below).

## Run on a remote machine

```python
from kkrunner import run

run("hostname", target="user@cluster")
```

The same API works over SSH. By default, working directories under your
home directory map to the corresponding location on the remote host.
Direct SSH execution requires only Python on the remote machine.

KKRunner can also upload input files and bring back the results:

```python
run(
    ["cat", "input.txt"],
    target="user@cluster",
    input_files="input.txt",
    output_file="output.txt",
)
```

The local `input.txt` is uploaded and the output is downloaded automatically.
Use `output_files` for additional result files.
Combine SSH with `backend="kkrunner"` for reconnectable remote jobs
(requires KKRunner on the remote machine).

## Run on a computing cluster with PSI/J

```python
from kkrunner import run

run(["echo", "Hello"], backend="psij", output_file="slurm.out")
```

This submits a job to Slurm and waits for its completion. As with other
backends, you can use `submit()` instead, or add `target="user@cluster"`
to submit remotely.

For example, to run an MPI application through the remote scheduler:

```python
from kkrunner import submit

job = submit(
    "my_mpi_program",
    target="user@cluster",
    backend="psij",
    mpi=8,
    output_file="mpi.out",
)
```

Scheduler execution requires KKRunner with the PSI/J extra and a configured
scheduler on the execution host. See [installation](#installation).

## Keep a history of calculations

Add `tracked=True` to any command to save its history. Tracking is off by
default, regardless of the backend.

Organize calculations into projects:

```python
from kkrunner import ProjectRepository, ProjectRunManager, run

project_id = ProjectRepository().create("My project").id
manager = ProjectRunManager(project=project_id)

run(["echo", "Hello"], tracked=manager, output_file="project.out")
```

Reuse the same project ID for later calculations, including from other
Python sessions. See [projects](docs/projects.rst) for more.

## Monitor jobs from the command line

For the tracked background job above, find the history directory once:

```python
from kkrunner import RunManager

print(RunManager.get_default().store.directory)
```

Then use that directory and the job's ID in your shell:

```sh
kkrunner --store /path/to/runs follow RUN_ID --from-start
```

Use `status` or `cancel` instead of `follow` to check or stop a job.
The same commands work for local, SSH and scheduler jobs.
See [CLI options](docs/command-line.rst).

## Choose how jobs are executed

Choose the backend independently of the execution location. All three
backends use the same Python API, locally or over SSH.

| Backend | When to use it |
| --- | --- |
| `direct` (default) | Run a command directly, without a scheduler or supervisor. |
| `kkrunner` | Run independent jobs that you can reconnect to, with a retained exit status. |
| `psij` | Submit jobs to Slurm (the default) or another configured PSI/J executor. |

For scheduler configuration, MPI launchers and recovery capabilities, see
[execution backends](docs/backends.rst).

## Synchronize files over SSH

Only the files you declare are transferred, rather than the entire working
directory. Use filename patterns for sets of files and `cache_files` for
files a program can reuse and update between runs.

For example, run a calculation in a remote scratch directory and bring
its results back to your local working directory:

```python
from kkrunner import StagingSpec, run

run(
    "my_program",
    target="user@cluster",
    staging=StagingSpec(local_root=".", remote_root="/scratch/user/work"),
    input_files=["input.dat", "parameters/*.txt"],
    output_files="results/*.dat",
    cache_files="cache.bin",
)
```

By default, synchronization avoids transferring unchanged files. You can
also transfer every file, fill only missing files or disable transfers,
independently for uploads and downloads. Compressed batch transfers and
SSH connection reuse reduce overhead.

See [file declarations and synchronization](docs/files.rst) and
[remote execution](docs/remote.rst) for mapping directories, transfer
policies, compression and SSH configuration.

## More than running commands

Beyond starting commands, KKRunner can help your application:

- **Run jobs safely in parallel:** prevent conflicting reads and writes of
  declared files by tracked jobs sharing a run store.
- **Display progress:** receive live stdout and stderr through a
  callback, or inspect a declared result file while a tracked job runs.
- **Recover interrupted workflows:** reconnect to an existing job, resume
  failed downloads when recoverable, or retry result processing without
  rerunning the calculation.
- **Process results automatically:** pass a Python `process_result` function
  that turns the execution outcome into an application-specific result.
- **Manage results:** keep file versions and checksums, detect replaced
  historical results and organize runs into a project hierarchy.

See [persistent runs and recovery](docs/persistent-runs.rst),
[working with files](docs/files.rst), [projects](docs/projects.rst) and
the [Python quickstart](docs/quickstart.rst).

## Waiting, detachment, reattachment and tracking

These four properties describe different aspects of a job:

- **Waiting:** `run()` waits for the result; `submit()` returns a job you can
  monitor or wait for later.
- **Detachment:** the job keeps running when your Python session exits.
  Supervisor and scheduler jobs are detached; direct jobs can opt in with
  `detached=True`.
- **Reattachment:** reconnect to an existing job without restarting it.
  The supervisor and scheduler can also recover the exit status when retained.
  With the direct backend, reattachment polls saved output files instead of
  using an efficient live stream. Save stdout and stderr separately to follow
  both; the original exit code is unavailable after reconnection.
- **Tracking (`tracked=True`):** saves job details, status and file records
  under a run ID. It also protects declared files against conflicting tracked
  jobs in the same store. Tracking neither detaches a job nor changes the
  backend's reattachment capabilities.

Without tracking, you can still control or observe jobs using the backend's
own ID (`job.backend_id`), where supported. See [CLI options](docs/command-line.rst),
[backends](docs/backends.rst) and [run history and recovery](docs/persistent-runs.rst).

## Installation

Python 3.9 or newer. Install from the KKRunner checkout in a virtual environment:

```sh
python -m pip install .
# With scheduler support:
python -m pip install '.[psij]'
```

## Learn more

The [documentation](https://ase2sprkkr.github.io/kkrunner/) and
[source guide](docs/index.rst) provide more examples and the complete
[API reference](docs/api.rst).

For repeated commands, [configure an execution client](docs/client.rst) once.
For advanced lifecycle control, see the [Python quickstart](docs/quickstart.rst).

[Development and tests](docs/development.rst) · [Architecture](docs/architecture.md) · [MIT license](LICENCE)
