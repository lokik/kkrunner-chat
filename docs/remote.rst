Remote execution
==================

``EndpointClient`` uses its transport to select the execution location. The backend independently
selects direct execution, a KKRunner supervisor or PSI/J. Direct execution
needs Python on the target. Supervisor and PSI/J helpers import the installed
KKRunner library there; PSI/J also needs its optional dependency and scheduler.
No separate agent endpoint or persistent agent service is required.

.. code-block:: python

   from pathlib import Path
   from kkrunner import ExecutionSpec, RunManager, RunStore
   from kkrunner.backends import SupervisedBackend
   from kkrunner.client import EndpointClient, SshTransport, StagingSpec

   directory = Path("remote-example").absolute()
   directory.mkdir(exist_ok=True)
   manager = RunManager(RunStore(directory / "runs"))
   client = EndpointClient(SupervisedBackend, transport=SshTransport("user@cluster"))
   spec = ExecutionSpec(
       ["python3", "-c", "print(42)"], directory=directory, output_file="stdout.txt",
       staging=StagingSpec(local_root=directory, remote_root="/absolute/remote/work"),
   )
   run = manager.create_run(client, spec)
   run.run()

For a one-shot command use ``EndpointClient(DirectBackend, transport=transport)``.
For a scheduler use ``EndpointClient(PsijBackend(executor_name="slurm"), transport=transport)``.
The client and file session stay the same. Direct local process identities use
PID and start time, but direct execution currently offers no independent
reattachment or authoritative exit status after losing its observer.

Select the target Python environment on the transport:

.. code-block:: python

   client = EndpointClient(
       SupervisedBackend,
       transport=SshTransport("user@cluster", python_command=("/opt/kkrunner/bin/python",)),
   )

Select storage identity in the run's file mapping, independently of this
connection:

.. code-block:: python

   staging = StagingSpec(
       local_root=directory,
       remote_root="/absolute/remote/work",
       filesystem_id="cluster-home",
   )
   # Pass staging=staging when constructing ExecutionSpec.

A consistent ``StagingSpec.filesystem_id`` declares that aliases or shared mounts name the
same target filesystem, independently of backend and SSH options. Workspace
mapping belongs to each run's ``StagingSpec``. The target directory and complete
file footprint are resolved before reservation; uploads, output opening and
submission start after reservation. Declare every read, write and cache;
undeclared executable side effects cannot be protected by file reservations.

The SSH transport obtains the selected backend's ``SshHelper`` and transfers
one method definition. Direct uses its own portable method. Supervisor and
PSI/J use import-only helpers. Local execution calls the original methods,
without another Python interpreter. ``DirectOperations`` and ``ManagedOperations``
define static namespace interfaces, with JSON request/reply types in
``kkrunner.backends.support.messages``. Type annotations are erased when extracting
portable source; the executed method body is shared.

Managed FOLLOW forwards calculation frames and terminal facts, keeping SSH
diagnostics separate. Disconnecting a follower preserves the supervisor and
calculation; explicit backend cancellation is a separate operation. Direct
stdout/stderr use nonblocking descriptor writes so output backpressure cannot
stall its cancellation reader. Pending output may continue draining after
calculation termination.

Transport operations are asynchronous and transfer exact files. Wildcards are
expanded on the source side and paths remain relative to the selected root.
Durable run completion, synchronization and receipt acknowledgements remain
client responsibilities. See :doc:`persistent-runs` for recovery requirements.
