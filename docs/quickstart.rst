Run a program
===============

An :class:`~kkrunner.ExecutionSpec` describes a command and its files.
A :class:`~kkrunner.Run` coordinates a backend job; ``run()`` submits and waits,
while ``submit()`` followed by ``wait()`` separates those steps.
Commands are argument sequences, so shell expansion is not performed.

.. testcode::

   import sys
   import tempfile
   from pathlib import Path
   from kkrunner import ExecutionSpec
   from kkrunner import Run
   from kkrunner.backends import DirectProcessBackend

   workspace = tempfile.TemporaryDirectory()
   directory = Path(workspace.name)
   spec = ExecutionSpec(
       [sys.executable, "-c", "print(42)"],
       directory=directory,
       output_file="stdout.txt",
   )
   runner = Run.direct(DirectProcessBackend(), spec)
   outcome = runner.run()
   print(outcome.return_code)
   print(Path(outcome.output_file).read_text().strip())

.. testoutput::

   0
   42

.. testcleanup::

   workspace.cleanup()

File paths in the specification are relative to ``directory``; here
``"stdout.txt"`` names the file inside the temporary working directory.
Absolute paths can be used when a file belongs elsewhere.

``outcome`` contains execution facts, including the backend return code and
stream paths. It contains no application-specific parsed result. Required
outputs must exist after a successful command; declare optional outputs with
:class:`~kkrunner.TaskFile` as described in :doc:`files`.

``Run.direct()`` alone does not submit the program. Output paths are opened at
submission. Supplied streams remain the caller's responsibility unless ownership
is explicitly transferred with :class:`~kkrunner.runs.base.ExecutionResources` and
:class:`~kkrunner.runs.base.ResourceOwnedRunner`.

For progress notifications, register ``runner.add_output_listener(callback)``
before submission. The callback receives ``(text, kind)``, where ``kind`` is
``"out"`` or ``"err"``. Output observers do not determine execution success;
use the runner's result and exception. A completion callback is a separate
notification and must not be confused with application result processing.

Use :class:`~kkrunner.RunManager` instead of a raw runner when the process must
be monitored again after the client exits. See :doc:`persistent-runs`.
