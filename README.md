# KKRunner

Program-independent Python execution, durable run history, file manifests,
local/MPI processes, SSH transport and optional PSI/J scheduler support.
The core depends on `platformdirs` and `psutil`; scheduler support is an optional extra.

```sh
python -m pip install .
python -m pip install '.[psij]'  # optional scheduler support
kkrunner --help
```

Run these commands from the KKRunner checkout in a virtual environment.
Use `python -m pip install --editable .` instead when editing the source.

## 1. Run a command locally

```python
import sys
from kkrunner import run

outcome = run([sys.executable, "-c", "print(42)"])
print(outcome.return_code)  # 0
```

By default, the command runs directly on the local machine. `run()` waits
for completion and displays unredirected output. Pass an argument list (or
a single executable name); shell expansion is not performed.

To save stdout and stderr instead of displaying them, pass
`output_file="stdout.txt"` and `error_file="stderr.txt"`.

## 2. Start a command and reconnect from another shell

Use `submit()` to start a command without waiting. Choose the built-in
supervisor (`backend="kkrunner"`) when a calculation must outlive the Python
client. It runs independently and retains completion status.

In the first shell, start Python and execute:

```python
import sys
from kkrunner import submit

job = submit(
    [sys.executable, "-u", "-c",
     "import time\nfor i in range(60):\n    print(i)\n    time.sleep(1)"],
    backend="kkrunner",
    output_file="supervised.out",
    error_file="supervised.err",
)  # start without waiting for completion
print(job.id)
```

Exit Python (e.g. Ctrl-D). The calculation continues. Copy the printed run ID
and, in a different shell on the same machine and under the same user account,
start Python again:

```python
from pathlib import Path
from kkrunner import RunManager

manager = RunManager.get_default()
job = manager.attach("RUN_ID")  # replace with the printed ID
outcome = job.wait()
print(outcome.return_code)
print(Path(outcome.output_file).read_text())
```

`attach()` reconnects to an existing nonterminal run, without submitting the
command again. If a record is already terminal, inspect it with
`manager.store.get("RUN_ID")` instead. Named output files allow output to be
replayed after disconnection. The initial observer is connected before the
supervisor starts the command; output listeners registered before `submit()`
receive the initial live output without a separate attach request. To detach
an active observer without cancelling the calculation, use
`job.stop_observation()`; `job.cancel()` requests cancellation instead.

The supervisor and PSI/J backends enable persistence and detached execution
by default. The direct backend does not provide the same durable reconnect
and exit-status guarantees, even when its process is detached. Keep the
supervisor's state, the run store and the working files available.

## 3. Run a command on another machine

Select an SSH target independently of the execution backend:

```python
from pathlib import Path
from kkrunner import run

run("hostname", target="user@cluster", directory=Path.home())
```

Replace `user@cluster` with an accessible SSH host. The direct backend only
needs Python on the target, not a KKRunner installation. By default, paths
under the local home directory map to the corresponding paths under the remote
home directory.

To upload input files and retrieve output files from a different working
directory, specify a file mapping:

```python
from pathlib import Path
from kkrunner import StagingSpec, run

work = Path("ssh-example").resolve()
work.mkdir(exist_ok=True)
(work / "input.txt").write_text("hello from the client\n")

outcome = run(
    ["cat", "input.txt"],
    target="user@cluster",
    directory=work,
    staging=StagingSpec(local_root=work, remote_root="kkrunner-example"),
    input_files=["input.txt"],
    output_file="output.txt",
)
print((work / "output.txt").read_text())
```

The remote root is relative to the remote home directory (an absolute path
can also be used). KKRunner uploads declared inputs and retrieves declared
outputs; additional result files can be listed in `output_files`.
Add `backend="kkrunner"` for remote supervised execution. That backend
requires KKRunner in the target Python environment.

## 4. Submit a job using PSI/J

Install the optional scheduler dependency in the execution environment:

```sh
python -m pip install '.[psij]'
```

On a host with a configured Slurm scheduler:

```python
from kkrunner import PsijBackend, run

outcome = run(
    ["python3", "-c", "print(42)"],
    backend=PsijBackend(executor_name="slurm"),
    output_file="slurm.out",
    error_file="slurm.err",
)
print(outcome.return_code)
```

The backend submits to the scheduler and waits for the job and its output.
`backend="psij"` is shorthand for the default Slurm configuration.
Add `target="user@cluster"` to submit via SSH; KKRunner, PSI/J and access to
the scheduler must then be available on the target. For MPI programs, pass
`mpi=8` (or the required process count). Use `submit()` instead of `run()` to return immediately after submission and
monitor the job separately.

## 5. Persist runs and group them in projects

Persistence records a stable run ID, backend identity, file manifest and
lifecycle state in a `RunStore`. Use `persistent=True` for the default user
store, or pass `persistent=RunManager(RunStore("/path/to/runs"))` to choose
a different store. Persistence does not require a project.

Projects group runs under a persistent project ID. Create a project once:

```python
from pathlib import Path
from kkrunner import ProjectRepository

projects = ProjectRepository()
work = Path("project-example").resolve()
work.mkdir(exist_ok=True)
project = projects.create("Example", directory=work)
print(project.id)
```

Use that project's ID for subsequent calculations, including from another
Python session:

```python
import sys
from kkrunner import ProjectRepository, ProjectRunManager, submit

projects = ProjectRepository()
project_id = "PROJECT_ID"  # replace with the ID printed above
project = projects.project(project_id)
manager = ProjectRunManager(project_repository=projects, project=project_id)

job = submit(
    [sys.executable, "-u", "-c",
     "import time\nfor i in range(60):\n    print(i)\n    time.sleep(1)"],
    directory=project.directory,
    output_file="project.out",
    error_file="project.err",
    backend="kkrunner",
    persistent=manager,
)
print("Run ID:", job.id)
print("Run store:", manager.store.directory)
```

The project's ID is recorded in `manager.store.get(job.id).project_id`.
All projects in this repository share a run store. The manager can reconnect
to an existing run via `manager.attach(run_id)`, just as in section 2.

## 6. Follow a saved run from the command line

Use the **run ID**, not the project ID or scheduler job ID, and the run store
directory printed above. In another shell:

```sh
kkrunner --store /path/to/runs status RUN_ID
kkrunner --store /path/to/runs follow RUN_ID --from-start
```

Without `--from-start`, `follow` displays only newly arriving output.
Use `status` to inspect execution state and `cancel RUN_ID` in place of
`status RUN_ID` to request cancellation. Following output is observation
only; it does not take over client-side downloads or result processing.
For an SSH run, the saved record supplies the target and backend; there is
no need to pass the hostname again.

## Further usage

Reuse an `ExecutionClient(target="user@cluster")` for repeated commands:
`client.run(command)` waits for completion, while `client.submit(command)`
returns a started `Run`. Both accept the same execution arguments. Use
`Run.create(ExecutionSpec(...))` or `client.create_run(...)` only when you
need an unstarted run, for example to configure observers before submission.

See the [user guide](docs/index.rst) for
[execution backends](docs/backends.rst), [remote execution](docs/remote.rst),
[file staging](docs/files.rst), [persistent runs and recovery](docs/persistent-runs.rst),
[projects](docs/projects.rst), and the [command-line interface](docs/command-line.rst).
The [architecture guide](docs/architecture.md) describes the internal design.

## Development and distribution

For development, install an editable checkout with test dependencies:

```sh
python -m pip install --editable '.[test]'
python -m pytest -q
```

To produce archives for sharing, install the build tool with
`python -m pip install build`, then run `python -m build`. This creates a source
archive and an installable wheel in `dist/`. CI validates these artifacts on
Linux with Python 3.9, 3.12 and 3.14, plus macOS tests on Apple Silicon and
Intel with Python 3.12, including durable local supervision and reconnect.
See [development](docs/development.rst) for test scope and packaging details.

## Publish to PyPI

The `publish-pypi.yml` workflow runs on a `v<version>` tag from any branch.
The tag must exactly match `[project].version` in `pyproject.toml`; publication
is independent of branch names.
The version is defined in `pyproject.toml`; `kkrunner.__version__` reads the
installed distribution metadata automatically.

Configure a PyPI Trusted Publisher for project `kkrunner`, GitHub owner
`ase2sprkkr`, repository `kkrunner`, workflow `publish-pypi.yml` and environment
`pypi`. Protect the GitHub `pypi` environment with required reviewers and `v*`
tag rules. If hosted under another owner, use that owner in
the publisher configuration. No PyPI API token is needed.

The workflow verifies the tag, builds one source archive and wheel,
then tests that wheel on Python 3.9, 3.12 and 3.14 and builds/tests the documentation.
Only after all checks pass does the publish job upload those exact artifacts
using the `pypi` environment and GitHub's OIDC token.

For nonpublic evaluation, use a prerelease version and do not push a version tag.
Branch CI retains wheel/sdist artifacts for evaluation. Use access-controlled
storage when sharing with testers: GitHub artifacts in a public repository are
not private, and an upload to PyPI is public even for a development version.

## Documentation

Documentation is published at <https://ase2sprkkr.github.io/kkrunner/> after
successful release-tag documentation checks.

The user guide and API reference use Sphinx with the Read the Docs theme.
Install the documentation extra and build the HTML and examples:

```sh
python -m venv .venv
.venv/bin/python -m pip install -e '.[docs]'
make docs PYTHON=.venv/bin/python
make docs-test PYTHON=.venv/bin/python
```

Use the same Python for dependency installation and Makefile targets; the
`docs` extra includes MyST and the theme. Open `build/docs/html/index.html`. See [the guide](docs/index.rst) for raw and
persistent execution, manifests, projects, schedulers, remote execution and the
CLI; [development](docs/development.rst) covers builds and releases. Generated
API pages and HTML are not committed. CI validates documentation against the
installed wheel and uploads the HTML artifact. The separate documentation
workflow checks the same wheel and documentation, omits the runtime test
matrices, and deploys to GitHub Pages when a release is published or on manual runs. Enable
**Settings → Pages → Build and deployment → GitHub Actions** and allow release
tags and manually selected source branches in the `github-pages` environment
deployment rules.

Source provenance is recorded in [development remarks](docs/remarks/source-provenance.md);
the license is in [LICENCE](LICENCE).
