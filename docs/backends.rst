Choose an execution backend
=============================

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - Backend
     - Use
     - Requirements and limits
   * - ``DirectProcessBackend``
     - A direct subprocess on the client machine
     - No durable handle or reattachment.
   * - ``SupervisedProcessBackend``
     - A reconnectable process on the client machine
     - POSIX process groups and Unix sockets, plus retained supervisor receipts.
   * - ``PsijBackend``
     - A scheduler visible from the client
     - PSI/J extra and scheduler configuration; scheduler IDs are backend facts.
   * - ``SshDirectRunEndpoint``
     - A one-shot command on an SSH host, without a target KKRunner installation
     - SSH and Python 3 on the target; no durable handle or reconnect. Uses the
       same mapped input/output file lifecycle as an agent run.
   * - ``AgentRunEndpoint``
     - Direct execution or a scheduler on an SSH target
     - KKRunner on the target, SSH access and explicit workspace mapping.


Target-side backend code and SSH helpers
----------------------------------------

All target backend operations are declared as static methods on their
backend class. For each supported backend the SSH endpoint applies its
`SshHelper` and transmits the Python source of the selected static method only. The helper
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
     - A standalone process worker of roughly 120 lines plus shared MPI
       resolution and output protection
   * - Supervised
     - Import-only helper
     - Installed KKRunner and a POSIX execution host
     - Dedicated supervisor worker, socket streams, durable receipts and
       status/recovery operations (roughly 1,000 lines total)
   * - PSI/J
     - Import-only helper
     - Installed KKRunner with `psij-python` and the scheduler integration
     - Native PSI/J job, executor, scheduling/accounting and operation modules
       (roughly 1,150 lines total)

Transferring the full supervised or PSI/J runtime would duplicate installed
dependencies and complicate persistence and recovery. For any selected operation, their SSH helper source contains one delegated
method import; actual code is imported on the target host. Direct SSH is deliberately independent and sends the compact worker
source. Calculation stream FOLLOW uses framed data to distinguish native
stdout/stderr from SSH diagnostics and delivers a terminal receipt.

Direct SSH execution
--------------------

``SshDirectRunEndpoint`` creates an untracked, connection-owned ``DirectProcessJob``. No
``kkrunner-agent`` or supervisor runs on the target; a minimal one-shot
Python worker provides separate live stdout/stderr, cancellation and target-side
file writing. File staging and synchronization still use ``RemoteFileSession``.
Both directions of transfer use SSH; agentless file metadata queries and
atomic upload publication execute source-transferred, stdlib-only Python functions.
The same ``DirectProcessJob`` handles direct local and direct SSH process lifecycles;
only the launch/stop/output adapter differs. The target Python command can be
configured with ``SshDirectTransport(python_command=("python3.12",))``.

.. code-block:: python

   from kkrunner import Run, ExecutionSpec
   from kkrunner.client import SshDirectRunEndpoint, SshDirectTransport
   from kkrunner.runs import LaunchSpec

   backend = SshDirectRunEndpoint(SshDirectTransport("alice@cluster.example"))
   spec = ExecutionSpec(
       argv=("python3", "-c", "print('hello', flush=True)"),
       directory="/home/alice/work/job",
       output_file="/home/alice/work/job/output.txt",
       launch=LaunchSpec(
           local_root="/home/alice/work",
           remote_root="/scratch/alice/runs",
       ),
   )
   result = Run.direct(backend, spec).run()

Use ``AgentRunEndpoint`` and a ``RunManager`` for persistent SSH
runs. A direct SSH Job has no durable ID and must not be attached, retried after
an uncertain connection loss, or stored as a managed run. An SSH failure (exit
255) is reported as UNKNOWN, not proof that the target computation failed.

Local and scheduler execution
-------------------------------

Use ``Run.direct(DirectProcessBackend(), spec)`` for an untracked run. Use
``RunManager.create_run(SupervisedProcessBackend(), spec)`` for durable local
supervision; the execution mode does not depend on whether a submission key
was supplied. The same manager interface accepts a scheduler executor:

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

:class:`~kkrunner.client.ExecutionEndpointConfig` contains backend and connection identity.
It is serializable and hashable; :class:`~kkrunner.client.ExecutorPool` reuses instances
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

Architecture boundary
---------------------

The native ``Backend`` contract creates or opens a ``Job`` and does not
import client-side sessions. A client ``RunEndpoint`` composes the Job with a
file session as ``RunParts``. Native process and PSI/J backends are adapted by
``BackendRunEndpoint``; remote proxies compose their own sessions.

The file session prepares mapped paths and a manifest, but does not decide
which executor runs or whether the submission is detached. The endpoint builds
the backend request and selects durability policy. ``Run`` owns completion,
synchronization and application artifact verification; ``RunManager`` persists
durable state and history.

Backend status queries report backend facts only. A remote backend reporting
finished does not itself imply the client Run is complete: RunManager
maps it to downloading until the Run synchronizes output and finishes
optional postprocessing.

Direct local and direct SSH processes share the same DirectProcessJob lifecycle.
Their adapters receive command specifications and processes and report transport
observations; required application output verification belongs to the client,
not to a native Job. The host-side MPI launcher decision and safe deletion of
stale output files are shared with supervised and PSI/J launch paths.
SSH-direct transfers these independent native functions as source when no
KKRunner is installed on the target.

``RunParts`` owns the transfer-resource handoff to the Run completion observer,
including failed attachment cleanup. Supervised execution retains its installed
KKRunner agent and detached supervisor for receipts and recovery.

Runtime ownership and immutable declarations
-------------------------------------------

The public ``Run.spec`` is the original execution declaration, not a scratch
space for active file streams or transport resources. For direct processes the
``Job`` retains the original specification and the adapter acquires runtime
output streams and resolves MPI immediately before launching the process.
For remote runs, ``RemoteFileSession`` owns client-side stream handles and
transfer offsets, without replacing the immutable specification.

Live backend identifiers and process/connection handles belong to the
``Job`` (and its adapter) whether or not a ``RunManager`` persists them.
A persistent ``RunRecord`` stores only the identity needed to reconnect
supported jobs. Attached specifications can be partial (``argv=None``) and
describe known file paths, never newly opened runtime stream objects.
