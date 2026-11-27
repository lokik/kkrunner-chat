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

## Keep a history of calculations

Supervised and scheduler jobs are saved automatically. Add
`persistent=True` to save a directly executed job as well.

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

## Installation

Python 3.9 or newer. Install from the KKRunner checkout in a virtual environment:

```sh
python -m pip install .
# With scheduler support:
python -m pip install '.[psij]'
```

## Learn more

The [documentation](https://ase2sprkkr.github.io/kkrunner/) and
[source guide](docs/index.rst) cover custom SSH and scheduler configurations,
MPI launchers, live output callbacks, file staging and caching, project
management, recovery and application-specific result processing.

For repeated commands, configure an `ExecutionClient` once and use its
`run()` and `submit()` methods. For advanced lifecycle control, use
`Run.create()` to prepare a job before submission.

[Development and tests](docs/development.rst) · [Architecture](docs/architecture.md) · [MIT license](LICENCE)
