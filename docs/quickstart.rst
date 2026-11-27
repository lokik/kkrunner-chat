Run a program
===============

For a blocking local command, import :func:`kkrunner.run`.

.. testcode::

   import sys
   from kkrunner import run

   outcome = run([sys.executable, "-c", "print(42)"])
   print(outcome.return_code)

.. testoutput::

   42
   0

Commands are argument sequences, or a single executable pathname. Shell
expansion is never performed. The function displays available unredirected
output. The supervised backend waits for its first observer when a stream
has no named file. It can start without an observer after a bounded grace
period if the client disappears; missed live-only bytes cannot be replayed.
For PSI/J or detached direct execution, declare named output files to capture
their streams. Named files are also necessary for replay after disconnect.

Use a target address to run on another computer, and select the backend
independently. The default is direct execution, local or over SSH.

.. code-block:: python

   from kkrunner import run

   run("program", target="user@cluster", mpi=8)
   run("program", target="user@cluster", backend="psij",
       output_file="stdout.txt")

Prepare a Run
--------------

:class:`~kkrunner.ExecutionSpec` describes a command and its files.
:class:`~kkrunner.Run` coordinates a backend job; ``run()`` submits and waits,
while ``submit()`` followed by ``wait()`` separates those steps.

.. testcode::

   import sys
   import tempfile
   from pathlib import Path
   from kkrunner import ExecutionSpec, Run

   workspace = tempfile.TemporaryDirectory()
   spec = ExecutionSpec(
       [sys.executable, "-c", "print(42)"],
       directory=workspace.name,
       output_file="stdout.txt",
   )
   runner = Run.create(spec)
   outcome = runner.run()
   print(outcome.return_code)
   print(Path(outcome.output_file).read_text().strip())

.. testoutput::

   0
   42

.. testcleanup::

   workspace.cleanup()

For repeated commands, construct an :class:`~kkrunner.ExecutionClient` once:

.. code-block:: python

   from kkrunner import ExecutionClient

   client = ExecutionClient(target="user@cluster")
   runner = client.create_run("program")
   runner.submit()
   result = runner.wait()

File paths in the specification are relative to ``directory``; absolute paths
can be used when a file belongs elsewhere. A single path is accepted for
``input_files``, ``output_files`` and ``cache_files``. MPI may be specified
as a process count, a launcher or an explicit MpiSpec.

``outcome`` contains execution facts, including the backend return code and
stream paths. It contains no application-specific parsed result. Required
outputs must exist after a successful command; declare optional outputs with
:class:`~kkrunner.TaskFile` as described in :doc:`files`.

``Run.create()`` alone does not submit the program. Output paths are opened at
submission. To transfer additional resources to the Run, pass
``owned_resources=ExecutionResources(...)`` to ``Run.create()`` or ``ExecutionClient.create_run()``. Ownership transfers when
the factory returns successfully; the caller cleans up if construction fails.
Run releases these resources after execution, output synchronization and
postprocessing have settled, including when nobody calls ``wait()``.
An unstarted Run can release its resources with ``close()`` or ``cancel()``.
Resources remain owned when observation ends with an uncertain outcome or
postprocessing is still pending; ``close()`` rejects those states. Ownership
is local to this Run object and is not serialized into a persistent record.
After reconnection completes a tracked run, ``refresh()`` followed by ``close()``
can release resources retained by the original observer.
Supplied streams retain their existing ownership rules; see :doc:`files`.

For progress notifications, pass ``output_listener=callback`` when creating the run, or register
``runner.add_output_listener(callback)`` before submission. The callback receives ``(text, kind)``, where ``kind`` is
``"out"`` or ``"err"``. Output observers do not determine execution success;
use the runner's result and exception. A completion callback is a separate
notification and must not be confused with application result processing.

Choose a managed backend when the process must be monitored again after the
client exits. Use ``RunManager.get_default().attach(run_id)`` to reconnect. See :doc:`persistent-runs`.
