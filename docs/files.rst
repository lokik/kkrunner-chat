Declare the files a run uses
==============================

A file manifest lists the files a run may read, create or modify. KKRunner
builds it from the file fields of :class:`~kkrunner.ExecutionSpec` and uses it
for transfers, output checks, captured file versions and managed-run
reservations. Declare all files the program uses; these declarations do not
restrict the process's filesystem access.

File paths are relative to ``ExecutionSpec.directory``. Use a plain filename
such as ``"stdout.txt"`` for a file in that directory, or an absolute path
for a file elsewhere.

* ``input_file`` identifies the program's main input file for tracking and
  transfer. The command must still pass its name to the program if needed;
  this field does not redirect standard input.
* ``stdin`` supplies the file whose contents the process reads through standard
  input. For example, ``stdin="input.txt"`` opens that file as the process's
  stdin. It may name the same file as ``input_file``.
* ``output_file`` and ``error_file`` save the process's stdout and stderr.
* ``input_files`` declares additional files read by the program.
* ``output_files`` declares additional files created by the program.
* ``cache_files`` declares files the program may both read and rewrite.

.. testcode::

   import sys
   import tempfile
   from pathlib import Path
   from kkrunner import ExecutionSpec, TaskFile
   from kkrunner.files import FileManifest

   workspace = tempfile.TemporaryDirectory()
   directory = Path(workspace.name)
   (directory / "input.txt").write_text("input")
   spec = ExecutionSpec(
       [sys.executable, "-c", "import sys; print(sys.stdin.read())"],
       directory=directory,
       input_file="input.txt",
       stdin="input.txt",
       output_file="stdout.txt",
       output_files=(TaskFile("optional.log", optional=True),),
       cache_files=("cache.bin",),
   )
   manifest = FileManifest.from_spec(spec)
   print(len(manifest.inputs), len(manifest.outputs), len(manifest.caches))
   print(manifest.outputs[0].optional)

.. testoutput::

   1 1 1
   True

.. testcleanup::

   workspace.cleanup()

``TaskFile`` is immutable. Its ``type`` and ``info`` strings belong to the
application; KKRunner does not look up file formats. Applications must
resolve pathname templates and callbacks before handing a concrete execution
specification to the runner. An optional declared file may be absent after
successful execution.

Reservations
--------------

Reservations prevent a managed run from overwriting files another managed run
still needs. They cover the declared read and write paths, including caches,
stdout/stderr and transfer destinations; sharing a working directory does not
reserve the entire directory.

A runner created by ``RunManager.create_run`` reserves its files at ``submit()``,
before uploads, opening outputs or starting the command. The reservation lasts
until the managed run reaches a terminal state, including any transfers and
required result processing. Detaching observation does not release it.

Coordination applies to runs in the same ``RunStore``, even when different
manager instances or projects use that store. ``ProjectRunManager`` uses
the same mechanism. Untracked ``Run.untracked`` calls and separate stores do
not coordinate these reservations.

Two active runs may read the same file. A write conflicts with another run's
read or write. Cache declarations participate in both roles. Local uploads,
download destinations and streamed output paths are checked independently of
the backend. Remote paths are also checked in their target filesystem namespace.
A :class:`~kkrunner.runs.DuplicateSubmissionError` reports an active conflict.

For host aliases or shared mounts, supply the same ``filesystem_id`` to
:class:`~kkrunner.client.StagingSpec`. It is persisted in FileMapping and
does not partition connection identity. KKRunner does not infer shared mounts
from different hostnames. Distinct IDs declare distinct target filesystems.

Transfers and captured contents
---------------------------------

:class:`~kkrunner.client.StagingSpec` provides the local/remote roots and transfer
policies. ``changed`` transfers differing contents; ``always`` transfers every
existing source; ``missing`` fills only missing destinations; ``never`` disables
that transfer direction. Missing required inputs remain errors.

Successful durable completion captures file versions and SHA-256 digests.
Historical applications should verify captured files before interpreting them.
:class:`~kkrunner.runs.HistoricalFileMismatchError` indicates that historical content
was replaced. A missing required local output is an ``ARTIFACTS`` failure;
a failed remote transfer is a ``DOWNLOAD`` failure. Optional missing outputs
are recorded as skipped.

Read files during a running calculation
----------------------------------------

``RunManager.snapshot_file(run, file)`` reads the currently visible contents
of a declared output, saved stdout/stderr stream, or cache. It works while the
calculation is running and after completion. A tracked ``Run`` also offers
``run.snapshot_file(file)``.

.. code-block:: python

   current_bytes = run.snapshot_file("progress.dat")
   copy_path = run.snapshot_file(
       "progress.dat", destination="/tmp/progress-snapshot.dat",
   )

The selected file is given using the **local** working-directory namespace,
including for remote runs. A wildcard declaration may select an existing
concrete filename, but the snapshot request must name one file. Inputs or
undeclared paths cannot be read through this API.

Without ``destination``, the result is ``bytes``, which loads the contents
into memory. With ``destination``, the result is a ``pathlib.Path``; files
are streamed to a temporary destination and atomically published on success.
Snapshot destinations must not overlap declared run files. The original
files, persisted run state, availability and historical checksums are not
modified. The remote source is read **on the execution host**, independently
of any local output already synchronized.

**A snapshot is not a consistent point-in-time copy.** The producer may
continue appending or rewriting while it is being read. This observational
API intentionally differs from the verified, terminal-only
``retrieve_result_files()`` and final output synchronization.
