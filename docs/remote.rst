Remote execution
==================

``ExecutionClient`` uses its transport to select the execution location. The backend independently
selects direct execution, a KKRunner supervisor or PSI/J. Direct execution
needs Python on the target. Supervisor and PSI/J helpers import the installed
KKRunner library there; PSI/J also needs its optional dependency and scheduler.
No separate agent endpoint or persistent agent service is required.

.. code-block:: python

   from pathlib import Path
   from kkrunner import ExecutionSpec, RunManager, RunStore
   from kkrunner.backends import SupervisedBackend
   from kkrunner.client import ExecutionClient, StagingSpec
   from kkrunner.targets import SshTarget

   directory = Path("remote-example").absolute()
   directory.mkdir(exist_ok=True)
   manager = RunManager(RunStore(directory / "runs"))
   client = ExecutionClient(SupervisedBackend, target=SshTarget("user@cluster"))
   spec = ExecutionSpec(
       ["python3", "-c", "print(42)"], directory=directory, output_file="stdout.txt",
       staging=StagingSpec(local_root=directory, remote_root="/absolute/remote/work"),
   )
   run = client.create_run(spec, persistent=manager)
   run.run()

For a one-shot command use ``ExecutionClient(DirectBackend, target=transport)``.
For a scheduler use ``ExecutionClient(PsijBackend(executor_name="slurm"), target=transport)``.
The client and file session stay the same. Direct execution can reconnect to
saved output by polling files, but cannot recover the original exit status
once the submitting observer is lost. Declare separate stdout and stderr
files to recover both output channels.

Select the target Python environment on the transport:

.. code-block:: python

   client = ExecutionClient(
       SupervisedBackend,
       target=SshTarget("user@cluster", python_command=("/opt/kkrunner/bin/python",)),
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

By default, the SSH transport pools up to four SSH sessions, each with a
persistent remote Python interpreter. Portable functions and their transitive
dependencies are installed once per live interpreter under stable names derived
from their module and qualified names. The client tracks installed identities;
the remote side uses ordinary Python globals, without a separate registry.
Idle interpreters close after 120 seconds. The pool is shared across endpoint
clients using identical transport settings, irrespective of execution backend.
The sessions are independent of persistent run records and require only
Python on the remote machine. Long-lived streaming FOLLOW/execution observers
and bulk shell-based transfers continue using independent SSH commands; they
do not consume RPC worker capacity. Set ``pool_size=0`` to use one-shot RPCs,
or configure ``pool_size`` and ``pool_idle_seconds`` on ``SshTarget``.
Portable code is immutable for the lifetime of a pool: restart the pool after
reloading code. Closing the client or transport closes its shared pool.

The SSH transport selects native portable direct methods or generates an
import-only wrapper for managed backend methods, then transfers the
``@portable``-marked source and its declared dependencies.
Direct uses its own method. Supervisor and PSI/J use generated, import-only
helpers. Local execution calls the original methods, without another Python
interpreter. ``DirectOperations`` and ``ManagedOperations`` define static
namespace interfaces, with JSON request/reply types in
``kkrunner.backends.support.messages``. Client decorators and type
annotations are erased when extracting source; direct-backend portability is
validated by tests rather than during source delivery.

Managed FOLLOW forwards calculation frames and terminal facts, keeping SSH
diagnostics separate. Disconnecting a follower preserves the supervisor and
calculation; explicit backend cancellation is a separate operation. Direct
stdout/stderr use nonblocking descriptor writes so output backpressure cannot
stall its cancellation reader. Pending output may continue draining after
calculation termination.

Target operations are asynchronous and transfer exact files. Wildcards are
expanded on the source side and paths remain relative to the selected root.
Durable run completion, synchronization and receipt acknowledgements remain
client responsibilities. See :doc:`persistent-runs` for recovery requirements.

Exact batch transfers
---------------------

Low-level transport batch methods take exact relative paths, or explicit
source/destination pairs, with one directory per filesystem. For example::

    await transport.put_files(
        ["TAU/a.dat", "TAU/b.dat", ("input.pot", "start.pot")],
        source_directory="/home/user/project",
        destination_directory="/scratch/project",
    )

The same contract applies to ``get_files()``. The file context expands patterns;
targets never expand them a second time. SSH streams the compact file list
through standard input, not command-line arguments, and sends one archive per
batch. Each received file is staged on its destination filesystem and published
only after the archive is complete. Publication is atomic per file, not a
filesystem-wide transaction.

SSH archive compression
-----------------------

``SshTarget(compression="auto")`` selects Zstandard if the actual local and
configured remote Python both support tarfile's streaming Zstandard codec
(Python 3.14 or newer), otherwise gzip. The remote capability probe is cached
for the life of the transport. Explicit options are ``"gz"``, ``"bz2"``,
``"xz"`` and ``"zstd"``; an unsupported explicit codec is an error. Pass
``None`` for uncompressed archives. Batch and directory transfers use the
same Python standard-library streaming implementation; external tar or zstd
programs are not required.
