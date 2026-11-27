# KKRunner

**One Python API for running programs on your laptop, a remote machine or a
computing cluster.**

Run ordinary commands or MPI applications, launch jobs that survive the exit
of your Python session, reconnect to running jobs, transfer files over SSH
and keep a persistent history of your calculations.

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
    ["sh", "-c", "echo started; sleep 60; echo done"],
    backend="kkrunner",
    output_file="run.out",
    persistent=True,
)
print(job.id)
```

`submit()` returns as soon as the job is submitted. The built-in
`kkrunner` backend keeps the job running even after you exit Python.

While the job is running, reconnect from another Python session:

```python
from kkrunner import RunManager

job = RunManager.get_default().attach("RUN_ID")
result = job.wait()
```

Use the ID printed above. You can also monitor or cancel jobs from the
command line (see below).

## Run on a remote machine

```python
from kkrunner import run

run("hostname", target="user@cluster")
```

That's all you need to change. Run from a directory under your home directory;
KKRunner maps it to the corresponding location on the remote host.
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
scheduler on the execution host.

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

## Keep a history of calculations

Use `persistent=True` to save the run history, regardless of which backend
executes the command.

Organize calculations into projects:

```python
from kkrunner import ProjectRepository, ProjectRunManager, submit

project_id = ProjectRepository().create("My project").id
manager = ProjectRunManager(project=project_id)

job = submit(
    ["sh", "-c", "echo started; sleep 60; echo done"],
    backend="kkrunner",
    output_file="project.out",
    persistent=manager,
)
print(job.id)
print(manager.store.directory)
```

Reuse the same project ID for subsequent calculations, including from
other Python sessions. A project groups their run history and file records.

## Monitor jobs from the command line

Use the run ID and the run store directory printed above:

```sh
kkrunner --store /path/to/runs follow RUN_ID --from-start
kkrunner --store /path/to/runs status RUN_ID
kkrunner --store /path/to/runs cancel RUN_ID
```

The same commands work for saved local, SSH and scheduler jobs.

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

By default, file synchronization avoids unnecessary transfers. You can
also transfer every file, fill only missing files or disable transfers,
independently for uploads and downloads. SSH transfers use compressed
archives, and connections are reused across commands.

See [file declarations and synchronization](docs/files.rst) and
[remote execution](docs/remote.rst) for mapping directories, transfer
policies, compression and SSH configuration.

## Build your own calculation workflows

Beyond starting commands, KKRunner can help your application:

- **Run jobs safely in parallel:** prevent conflicting reads and writes of
  declared files by tracked jobs sharing a run store.
- **Display progress:** receive live stdout and stderr through an
  `output_listener`, or read an intermediate result using
  `job.snapshot_file("progress.dat")` on a tracked run.
- **Recover interrupted workflows:** reconnect to an existing job, resume
  recoverable downloads or retry result processing without rerunning the
  calculation.
- **Process results automatically:** pass a Python `process_result` function
  that turns the execution outcome into an application-specific result.
- **Manage results:** keep file versions and checksums, detect replaced
  historical results and organize runs into a project hierarchy.

See [persistent runs and recovery](docs/persistent-runs.rst),
[working with files](docs/files.rst), [projects](docs/projects.rst) and
the [Python quickstart](docs/quickstart.rst).

## Execution, detachment, reattachment and history

These are separate choices:

- **Waiting:** `run()` waits for the result; `submit()` returns a job you can
  monitor or wait for later.
- **Detachment:** a detached job keeps running when your client exits.
  Supervisor and scheduler jobs are detached; direct jobs can opt in with
  `detached=True`.
- **Reattachment:** depends on the backend. The supervisor and scheduler can
  reconnect to their jobs and retrieve the exit status when retained.
  Direct execution can follow saved output by polling files rather than a live
  stream. Save stdout and stderr in separate files to recover both channels;
  after reconnecting, the direct backend cannot retrieve the original exit code.
- **History (`persistent=True`):** saves a run ID, execution settings, status and
  file records. It enables file-conflict protection across tracked jobs in the
  same store and lets KKRunner restore the client-side run configuration from
  its ID. It does not change the backend's reattachment capabilities.

Without history, you can still use a supported backend's own `job.backend_id`
with the [CLI](docs/command-line.rst). See [backends](docs/backends.rst) and
[run history and recovery](docs/persistent-runs.rst).

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

For repeated commands, configure an `ExecutionClient` once and use its
`run()` and `submit()` methods. For advanced lifecycle control, use
`Run.create()` to prepare a job before submission.

[Development and tests](docs/development.rst) · [Architecture](docs/architecture.md) · [MIT license](LICENCE)
