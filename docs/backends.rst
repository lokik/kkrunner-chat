Choose an execution backend
=============================

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - Backend
     - Use
     - Requirements and limits
   * - ``DirectBackend``
     - A direct subprocess on the client machine
     - Boot-scoped process identity enables status and safe Linux cancellation; no reattachment or durable exit receipt.
   * - ``SupervisedBackend``
     - A reconnectable process on the client machine
     - POSIX process groups and Unix sockets, plus retained supervisor receipts.
   * - ``PsijBackend``
     - A scheduler visible from the client
     - PSI/J extra and scheduler configuration; scheduler IDs are backend facts.


Location and execution mode are independent. ``EndpointClient(BackendClass)``
and ``EndpointClient(BackendClass, transport=transport)`` construct configured backend
instances; both also accept an already configured instance. They default to
``DirectBackend``. ``client.backend`` is always an instance. Its immutable
``options`` and ``supports`` are computed when constructed; the backend class
still supplies the static native operations and ``SshHelper``. Location never
selects supervision.
``ExecutionMetadata.transport`` is an immutable ``LocalTransport`` or ``SshTransport``
combining connection settings with the operations that use them. ``ExecutionMetadata.backend`` separately stores a
``BackendConfig`` with an allowed name and immutable options. Neither field
contains a Python class name.


Target-side backend code and SSH helpers
----------------------------------------

All target backend operations are declared as static methods on their
backend class. The SSH transport selects ``type(client.backend)`` and applies its
``SshHelper`` and transmits only the selected method definition. The helper
does **not** create a new job state machine.

.. list-table::
   :header-rows: 1
   :widths: 18 24 38 20

   * - Backend
     - SSH mode
     - Target requirements
     - Target-side implementation
   * - Direct
     - Portable source (identity helper)
     - SSH and Python 3; no KKRunner installation
     - The same asynchronous process operation used by local execution;
       shared MPI resolution and output protection run before it
   * - Supervised
     - Import-only helper
     - Installed KKRunner and a POSIX execution host
     - Dedicated supervisor worker, socket streams, durable receipts and
       status/recovery operations
   * - PSI/J
     - Import-only helper
     - Installed KKRunner with `psij-python` and the scheduler integration
     - Native PSI/J submission, callback observation and scheduler/accounting operations

Transferring the full supervised or PSI/J runtime would duplicate installed
dependencies and complicate persistence and recovery. Their helper source
contains only delegated method imports; actual code is imported on the target
host. Direct SSH is deliberately independent and sends the compact worker
source. Calculation stream FOLLOW uses framed data to distinguish native
stdout/stderr from SSH diagnostics and delivers a terminal receipt.

Direct SSH execution
--------------------

``EndpointClient`` with ``backend=DirectBackend`` creates a connection-owned client ``Job``. No
``kkrunner-agent`` or supervisor runs on the target; a minimal one-shot
Python worker provides separate live stdout/stderr, cancellation and target-side
file writing. File staging and synchronization still use ``FileSession``.
Both directions of transfer use SSH; agentless file metadata queries and
atomic upload publication execute source-transferred, stdlib-only Python functions.
The same generic ``Job`` uses a transport-bound backend execution interface
locally and over SSH. The backend reports acceptance and terminal status;
SSH frames carry those facts alongside output, separately from transport
diagnostics and the SSH command's exit status. The target Python command can be
configured with ``SshTransport(python_command=("python3.12",))``.

.. code-block:: python

   from kkrunner import Run, ExecutionSpec
   from kkrunner.backends import DirectBackend
   from kkrunner.client import EndpointClient, SshTransport
   from kkrunner.client import StagingSpec

   client = EndpointClient(DirectBackend, transport=SshTransport("alice@cluster.example"))
   spec = ExecutionSpec(
       argv=("python3", "-c", "print('hello', flush=True)"),
       directory="/home/alice/work/job",
       output_file="/home/alice/work/job/output.txt",
       staging=StagingSpec(
           local_root="/home/alice/work",
           remote_root="/scratch/alice/runs",
       ),
   )
   result = Run.untracked(client, spec).run()

Use ``EndpointClient(SupervisedBackend, transport=transport)`` and a ``RunManager`` for persistent SSH
runs that need reconnection. Direct SSH retains the process identity for an
independent STATUS query, without supporting reattachment or submission recovery.
If its process (including surviving group members) has exited, the persisted
Run becomes FINISHED with an unknown return code and available files are
retrieved. This does not prove successful execution; postprocessing requires
confirmed success. If the SSH host cannot be queried, the job remains UNKNOWN.
Never automatically retry after uncertain connection loss.

Local and scheduler execution
-------------------------------

Use ``Run.untracked(EndpointClient(DirectBackend), spec)`` for an untracked run. Use
``RunManager.create_run(EndpointClient(SupervisedBackend), spec)`` for durable local
supervision; the execution mode does not depend on whether a submission key
was supplied. The same manager interface accepts a scheduler client:

.. code-block:: python

   from kkrunner.backends import PsijBackend
   from kkrunner.client import EndpointClient

   client = EndpointClient(PsijBackend(executor_name="slurm"))
   # manager and spec are created as in the persistent-run example.
   run = manager.create_run(client, spec)
   run.submit()

Backend instances and configured transports can be created without PSI/J
installed; executing a scheduler operation needs the extra. A scheduler job is only the backend part of
a run: downloads and application processing may continue after that job exits.

Transport identity and reuse
-----------------------------

:class:`~kkrunner.client.SshTransport` and
:class:`~kkrunner.client.LocalTransport` contain immutable connection settings. :class:`~kkrunner.backends.BackendConfig` stores
the separate backend selection. :class:`~kkrunner.client.ClientPool` keys clients
by both values and reconstructs supported clients after restart. Per-run launch
policy belongs in ``StagingSpec`` and must not split the transport's identity.
Close explicitly owned pools when their application scope ends.

.. _backend-recovery:

Recovery capabilities
-----------------------

A configured backend exposes an immutable set of capabilities through
``client.backend.supports``. ``REATTACH`` means reconnecting with a known
job ID is supported; ``RECOVER`` means the selected configuration implements
submission discovery without a known job ID. ``PsijBackend(executor_name="slurm",
url=None)`` supports both. Other PSI/J schedulers, and Slurm with an explicit
URL, support reattachment but not ``RECOVER``. Backend capabilities describe
available operations, not guaranteed success or retention of scheduler records.


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
   * - SSH direct (without agent)
     - Unsupported.
     - Unsupported.
   * - SSH via agent: process or scheduler execution
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
cannot yet resolve acceptance, the record stays ``submitting`` and its file
reservation remains held. Unsupported recovery changes the state to ``unknown``
without proving non-submission or releasing reservations. The manager does not automatically submit a second
command. Multiple matching scheduler jobs raise an error rather than selecting
one arbitrarily. Inspect the backend and restore its state before deciding
whether a new submission is safe.

MPI
-----

.. code-block:: python

   from kkrunner import ExecutionSpec
   from kkrunner.execution import MpiSpec

   spec = ExecutionSpec(["serial-program"], mpi_argv=["mpi-program"], mpi=MpiSpec.from_value(4))

The launcher is resolved on the execution host. Explicit required MPI cannot
fall back to serial execution. Autodetection may allow serial fallback and emit
:class:`~kkrunner.runs.MpiNotFoundWarning`; select that policy deliberately. KKRunner
does not supply MPI libraries or repair a binary's runtime linkage.

Architecture boundary
---------------------

Backend classes define static target operations; their configured instances
own the immutable ``options`` and ``supports``. They neither create client Jobs
nor receive client file manifests. ``EndpointClient`` holds a configured
backend instance and invokes its static methods. SSH selects
``type(client.backend).SshHelper`` and sends a single method definition. The client
creates the client ``Job`` and composes it with a file session as ``RunParts``.

The file session prepares mapped paths and a manifest, but does not decide
which backend runs or whether the submission is detached. The client builds
the backend request and selects durability policy. ``Run`` owns completion,
synchronization and application artifact verification; ``RunManager`` persists
durable state and history.

Backend status queries report backend facts only. A remote backend reporting
finished does not itself imply the client Run is complete: RunManager
maps it to downloading until the Run synchronizes output and finishes
optional postprocessing.

Direct local and direct SSH execution share the same client Job lifecycle.
Their adapters receive command specifications and processes and report transport
observations; required application output verification belongs to the client,
not to a native Job. The host-side MPI launcher decision and safe deletion of
stale output files are shared with supervised and PSI/J launch paths.
SSH-direct transfers these independent native functions as source when no
KKRunner is installed on the target.

``RunParts`` owns the transfer-resource handoff to the Run completion observer,
including failed attachment cleanup. Supervised execution retains an independent supervisor for receipts and
recovery. Local FOLLOW awaits the existing socket coroutine directly. SSH
starts a Python observer that calls the same backend FOLLOW and forwards
framed bytes. Cancelling observation closes only that follower; cancelling
the calculation requires the separate backend ``cancel`` operation. PSI/J
observation uses native status callbacks and releases them on detach.

Runtime ownership and immutable declarations
--------------------------------------------

The public ``Run.spec`` is the original execution declaration, not a scratch
space for active file streams or transport resources. For direct processes the
``Job`` retains the original specification and the adapter acquires runtime
output streams from its FileSession and invokes the backend MPI preflight
immediately before launching the process.
For all runs, ``FileSession`` owns client-side stream handles and
transfer offsets, without replacing the immutable specification.

Live backend identifiers and process/connection handles belong to the
``Job`` (and its adapter) whether or not a ``RunManager`` persists them.
A persistent ``RunRecord`` stores only the identity needed to reconnect
supported jobs. Attached specifications can be partial (``argv=None``) and
describe known file paths, never newly opened runtime stream objects.
