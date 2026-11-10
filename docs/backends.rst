Choose an execution backend
=============================

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - Backend
     - Use
     - Requirements and limits
   * - ``ProcessBackend``
     - A command on the client machine
     - Raw subprocesses have no reconnect guarantee; managed runs require POSIX process groups and Unix sockets.
   * - ``PsijBackend``
     - A scheduler visible from the client
     - PSI/J extra and scheduler configuration; scheduler IDs are backend facts.
   * - ``AgentBackendProxy``
     - Direct execution or a scheduler on an SSH target
     - KKRunner on the target, SSH access and explicit workspace mapping.

Local and scheduler execution
-------------------------------

Use ``Run.direct(ProcessBackend(), spec)`` for an untracked run, or pass the backend
to ``RunManager.create_run`` for durable supervision. The same manager interface
accepts a scheduler executor:

.. code-block:: python

   from kkrunner.backends import PsijBackend

   executor = PsijBackend(backend="slurm")
   # manager and spec are created as in the persistent-run example.
   run = manager.create_run(executor, spec)
   run.submit()

The PSI/J module can be imported without PSI/J installed; constructing a real
scheduler executor needs the extra. A scheduler job is only the backend part of
a run: downloads and application processing may continue after that job exits.

Endpoint identity and reuse
-----------------------------

:class:`~kkrunner.client.ExecutionEndpoint` contains backend and connection identity.
It is serializable and hashable; :class:`~kkrunner.client.BackendPool` reuses instances
for identical endpoints and can reconstruct them after restart. Per-run launch
policy belongs in ``LaunchSpec`` and must not split the executor's identity.
Close explicitly owned pools when their application scope ends.

.. _backend-recovery:

Recovery capabilities
-----------------------

Reconnecting with a stored backend ID and recovering an interrupted submission
are different operations. A crash during submission can leave a ``submitting``
record without that ID, even though the backend accepted the command.

.. list-table::
   :header-rows: 1
   :widths: 23 32 45

   * - Backend
     - Reconnect with a known ID
     - Recover submission without an ID
   * - Raw local subprocess
     - No durable reconnect guarantee.
     - Unsupported. Use a managed run when recovery is required.
   * - Managed local execution
     - Supported through the supervisor and its durable receipts.
     - Supported while the service retains the receipt indexed by the run ID.
   * - PSI/J Slurm, local scheduler access
     - Supported through the scheduler, subject to job retention.
     - Searches live jobs by the KKRunner run marker; completed jobs require
       Slurm accounting that stores job comments.
   * - Other PSI/J backends, or a PSI/J executor with a URL
     - Uses the selected PSI/J backend's attach/status capabilities.
     - Backend discovery is not implemented.
   * - SSH direct or SSH scheduler execution
     - Supported through the target service and its backend.
     - Uses a retained target receipt first. Without one, only the target's
       local Slurm recovery is implemented, with the same accounting limits.

Managed recovery requires the service state and working/output files to remain
available. Slurm discovery needs ``squeue`` and ``scontrol`` on the scheduler
host. Discovery after completion also needs ``sacct``, a recorded submission
time and ``AccountingStoreFlags`` containing ``job_comment``. The scheduler
must retain the corresponding records. SSH runs need those tools on the target.
These requirements concern discovery without an ID; they are not blanket
requirements for every attach operation.

``RunManager.recover_submitting(record)`` attempts recovery of a stale
``submitting`` record. Missing receipts, unavailable accounting and temporarily
missing accounting rows do not prove that submission failed. If the backend
cannot resolve acceptance, the record stays ``submitting`` and its file
reservation remains held. The manager does not automatically submit a second
command. Multiple matching scheduler jobs raise an error rather than selecting
one arbitrarily. Inspect the backend and restore its state before deciding
whether a new submission is safe.

MPI
-----

.. code-block:: python

   from kkrunner.runs import LaunchSpec, MpiSpec

   launch = LaunchSpec.from_mpi(4)

The launcher is resolved on the execution host. Explicit required MPI cannot
fall back to serial execution. Autodetection may allow serial fallback and emit
:class:`~kkrunner.runs.MpiNotFoundWarning`; select that policy deliberately. KKRunner
does not supply MPI libraries or repair a binary's runtime linkage.
