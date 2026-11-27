# KKRunner

Run commands locally, over SSH or through a job scheduler from Python.
KKRunner also supports MPI, file transfers, independent jobs and persistent
run history.

## Installation

From a KKRunner checkout, in a virtual environment:

```sh
python -m pip install .
```

## Run a command

```python
from kkrunner import run

result = run(["echo", "Hello"])
print(result.return_code)
```

`run()` waits for completion and displays the output. Use an argument list
or a single executable name. For shell expressions, invoke the shell explicitly.

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

`submit()` returns without waiting for completion. The `kkrunner` backend
keeps the job running independently of Python and saves its run ID by default.

Exit Python and reconnect from another shell on the same machine and account:

```python
from kkrunner import RunManager

job = RunManager.get_default().attach("RUN_ID")
result = job.wait()
print(result.return_code)
```

Replace `RUN_ID` with the printed ID. Output is saved in `run.out`.
If the saved run is already terminal, use
`RunManager.get_default().store.get("RUN_ID")` instead.

## Run on another machine

```python
from kkrunner import run

# Run from a directory under your home directory.
run("hostname", target="user@cluster")
```

The default backend runs the command directly over SSH. Only Python is
required on the target. Local paths under your home directory map to the
corresponding paths under the remote home directory.

Declare `input_files` and `output_files` to transfer files automatically.
For example, with a local `input.txt`:

```python
run(
    ["cat", "input.txt"],
    target="user@cluster",
    input_files="input.txt",
    output_file="output.txt",
)
```

This uploads `input.txt` and retrieves stdout as `output.txt`.
For a custom directory mapping, see [remote execution](docs/remote.rst).

Add `backend="kkrunner"` for an independent, reconnectable SSH job.
That backend requires KKRunner on the target.

## Run through a scheduler (PSI/J)

Install the scheduler extra on the execution host:

```sh
python -m pip install '.[psij]'
```

With a configured Slurm scheduler:

```python
from kkrunner import run

result = run(["echo", "Hello"], backend="psij", output_file="slurm.out")
print(result.return_code)
```

Use `target="user@cluster"` to submit via SSH,
`mpi=8` for an eight-process MPI program, or
`PsijBackend(executor_name="slurm")` to configure the scheduler explicitly
(import `PsijBackend` from `kkrunner`).
Use `submit()` instead of `run()` to return after submission.

## Save runs in a project

Supervised and scheduler jobs are persistent by default. For direct jobs,
use `persistent=True` to save run history; this does not make the direct
backend independently reconnectable with a guaranteed exit code.

Create a project once and keep its ID:

```python
from kkrunner import ProjectRepository

project_id = ProjectRepository().create("Example").id
print(project_id)
```

Use an existing project ID in any Python session:

```python
from kkrunner import ProjectRunManager, submit

manager = ProjectRunManager(project="PROJECT_ID")
job = submit(
    ["sh", "-c", "echo started; sleep 60; echo done"],
    backend="kkrunner",
    output_file="project.out",
    persistent=manager,
)
print(job.id)
print(manager.store.directory)
```

The run is saved under the specified project ID. Use
`manager.attach(run_id)` to reconnect to a nonterminal project run.

## Follow a run from the command line

Use the run ID (not the project ID) and the store directory printed above:

```sh
kkrunner --store /path/to/runs follow RUN_ID --from-start
kkrunner --store /path/to/runs status RUN_ID
kkrunner --store /path/to/runs cancel RUN_ID
```

Omit `--from-start` to follow only new output. The CLI uses the saved
target and backend. Following output does not take over result processing
or file downloads.

## More usage and documentation

For repeated jobs, reuse `ExecutionClient(target="user@cluster")` with
its `run()` and `submit()` methods. Use `Run.create()` or
`client.create_run()` when you need to prepare a run before submission.

See the [documentation](https://ase2sprkkr.github.io/kkrunner/) or the
[guide in this checkout](docs/index.rst) for
[backends and MPI](docs/backends.rst), [file staging](docs/files.rst),
[persistence and recovery](docs/persistent-runs.rst),
[projects](docs/projects.rst) and [CLI options](docs/command-line.rst).

## Development

```sh
python -m pip install -e '.[test]'
python -m pytest -q
```

See [development and documentation](docs/development.rst) for building
packages, generating documentation, CI, publishing to PyPI and GitHub Pages.

[Architecture](docs/architecture.md) · [Source provenance](docs/remarks/source-provenance.md) · [MIT license](LICENCE)
