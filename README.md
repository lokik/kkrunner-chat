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

## Raw execution

```python
import sys
from kkrunner import ExecutionSpec, Run
from kkrunner.backends import DirectBackend
from kkrunner.client import EndpointClient

run = Run.untracked(EndpointClient(DirectBackend), ExecutionSpec([sys.executable, '-c', 'print(42)']))
outcome = run.run()
assert outcome.return_code == 0
```

## Persistent execution

A `RunManager` records runs in a `RunStore` so a later client can reconnect.
Projects are optional; see the [project guide](docs/projects.rst) for grouping.

```python
import sys
from pathlib import Path
from kkrunner import ExecutionSpec, RunManager, RunStore
from kkrunner.backends import SupervisedBackend
from kkrunner.client import EndpointClient

root = Path('example').absolute()
root.mkdir(exist_ok=True)
manager = RunManager(RunStore(root / 'runs'))
run = manager.create_run(
    EndpointClient(SupervisedBackend),
    ExecutionSpec([sys.executable, '-c', 'print(42)'],
                  directory=root, output_file='stdout.txt'),
    metadata={'example': {'label': 'answer'}},
)
outcome = run.run()
assert manager.store.get(run.id).state.value == 'finished'
```

`manager.attach(run_id)` resumes monitoring and required transfers. A result
callback is optional; when supplied it receives an `ExecutionOutcome` and may
return any Python object. To defer processing, create a run with
`processing_required=True`, then supply `process_result=` on a later attach.
Without that callback the durable state remains `postprocessing` and `wait()`
raises `ProcessingPending`. The same exception tells an application observer
that another client owns/already performed processing; load the historical
application result instead of treating a raw outcome as a parsed result.

`manager.retry_postprocessing(run_id, callback)` retries failed/interrupted
processing against the captured files without submitting another process.
`manager.resume_download(run_id)` reopens a failed download before attach.
`run.stop_observation()` detaches observation without cancelling the backend;
`run.cancel()` requests backend cancellation. Untracked raw local runs have no
restart/reconnect guarantee; managed local runs use the same supervisor as SSH.

See the [documentation guide](docs/index.rst) and [architecture](docs/architecture.md)
for usage, ownership, recovery and format rules.

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
