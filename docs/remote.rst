Remote execution
==================

``SshRunEndpoint`` selects the execution location. The backend independently
selects direct execution, a KKRunner supervisor or PSI/J. Direct execution
needs Python on the target. Supervisor and PSI/J helpers import the installed
KKRunner library there; PSI/J also needs its optional dependency and scheduler.
No separate agent endpoint or persistent agent service is required.

.. code-block:: python

   from pathlib import Path
   from kkrunner import ExecutionSpec, RunManager, RunStore
   from kkrunner.backends import SupervisedBackend
   from kkrunner.client import SshRunEndpoint, SshTransport
   from kkrunner.runs import LaunchSpec

   directory = Path("remote-example").absolute()
   directory.mkdir(exist_ok=True)
   manager = RunManager(RunStore(directory / "runs"))
   endpoint = SshRunEndpoint(SshTransport("user@cluster"), backend=SupervisedBackend)
   spec = ExecutionSpec(
       ["python3", "-c", "print(42)"], directory=directory, output_file="stdout.txt",
       launch=LaunchSpec(local_root=directory, remote_root="/absolute/remote/work"),
   )
   run = manager.create_run(endpoint, spec)
   run.run()

For a one-shot command use ``backend=DirectBackend`` on the same endpoint.
For a scheduler use ``backend=PsijBackend`` and
``backend_options={"backend": "slurm"}``. The endpoint type and file-transfer
mechanism stay the same. A direct backend has no durable identity for reconnect;
client-side history alone cannot provide recovery after connection loss.

Select the target Python environment on the transport:

.. code-block:: python

   endpoint = SshRunEndpoint(
       SshTransport("user@cluster", python_command=("/opt/kkrunner/bin/python",)),
       backend=SupervisedBackend, filesystem_id="cluster-home",
   )

A consistent ``filesystem_id`` declares that aliases or shared mounts name the
same target filesystem, independently of backend and SSH options. Workspace
mapping belongs to each run's ``LaunchSpec``. The target directory and complete
file footprint are resolved before reservation; uploads, output opening and
submission start after reservation. Declare every read, write and cache;
undeclared executable side effects cannot be protected by file reservations.

The SSH endpoint obtains the selected backend's ``SshHelper`` and transfers
one method definition. Direct uses its own portable method. Supervisor and
PSI/J use import-only helpers. Local execution calls the original methods,
without another Python interpreter. ``DirectOperations`` and ``ManagedOperations``
define static namespace interfaces, with JSON request/reply types in
``kkrunner.backends.messages``. Type annotations are erased when extracting
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
