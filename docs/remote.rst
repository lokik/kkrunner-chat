Remote execution
==================

A remote executor separates a reusable target connection from each run's
workspace mapping. Install the same KKRunner version on the target; scientific
applications are needed there only when the command itself uses them.

.. code-block:: python

   from pathlib import Path
   from kkrunner import ExecutionSpec, RunManager, RunStore
   from kkrunner.runs import LaunchSpec
   from kkrunner.client import AgentRunEndpoint, SshTransport

   directory = Path("remote-example").absolute()
   directory.mkdir(exist_ok=True)
   manager = RunManager(RunStore(directory / "runs"))
   executor = AgentRunEndpoint(SshTransport("user@cluster"))
   spec = ExecutionSpec(
       ["python3", "-c", "print(42)"],
       directory=directory,
       output_file="stdout.txt",
       launch=LaunchSpec(
           local_root=directory,
           remote_root="/absolute/remote/work",
       ),
   )
   run = manager.create_run(executor, spec)
   run.run()

For managed runs, the target directory is resolved before reservation. Uploads,
output opening and submission start only after the declared files are reserved;
see :doc:`files` for the scope of this coordination. All reads,
writes and caches must be declared; undeclared executable side effects cannot
be protected by KKRunner's reservations.

For a nonstandard installation, pass an explicit protocol command:

.. code-block:: python

   executor = AgentRunEndpoint(
       SshTransport("user@cluster"),
       command=("/opt/kkrunner/bin/kkrunner-agent"),
       filesystem_id="cluster-home",
   )

Use a consistent ``filesystem_id`` when aliases or shared mounts name the same
target filesystem. It is independent of SSH flags and command selection.
For a scheduler on that target, supply ``backend="psij"`` and
``backend_options={"backend": "slurm"}`` (or another PSI/J backend) and install ``kkrunner[psij]`` on the target.

Test the protocol without SSH
-------------------------------

``LocalTransport`` executes the same remote protocol using local subprocesses.
It is useful for development and does not require a second machine:

.. testcode::

   import sys
   import tempfile
   from pathlib import Path
   from kkrunner import ExecutionSpec
   from kkrunner.runs import LaunchSpec
   from kkrunner import Run
   from kkrunner.client import AgentRunEndpoint, LocalTransport

   workspace = tempfile.TemporaryDirectory()
   directory = Path(workspace.name)
   local, remote = directory / "local", directory / "remote"
   local.mkdir()
   remote.mkdir()
   executor = AgentRunEndpoint(
       LocalTransport(), command=(sys.executable, "-m", "kkrunner_cli"),
   )
   outcome = Run.direct(executor, ExecutionSpec(
       [sys.executable, "-c", "print(44)"],
       directory=local,
       output_file="stdout.txt",
       launch=LaunchSpec(local_root=local, remote_root=remote),
   )).run()
   print(outcome.return_code)
   print((local / "stdout.txt").read_text().strip())

.. testoutput::

   0
   44

.. testcleanup::

   workspace.cleanup()

Transport operations are asynchronous and transfer exact files. Wildcards are
expanded on the source side, preserving paths relative to the source root.
Remote outcome receipts survive observation disconnects; durable run completion
and acknowledgements remain the client's responsibility. See
:doc:`persistent-runs` for download and processing recovery.
