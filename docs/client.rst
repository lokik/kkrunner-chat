Client, connection and backend
==============================

``ExecutionClient`` provides access to a configured backend through a transport.
It can serve multiple runs and composes their ``Job``, backend binding and
``FileSession``. There is one client implementation for local and SSH execution:

.. code-block:: python

   from kkrunner import ExecutionSpec, Run
   from kkrunner.backends import DirectBackend, PsijBackend
   from kkrunner.client import ExecutionClient
   from kkrunner.targets import SshTarget

   local = ExecutionClient(DirectBackend)
   remote = ExecutionClient(
       PsijBackend(executor_name="slurm"),
       target=SshTarget("user@cluster"),
   )
   run = local.create_run(ExecutionSpec(["python3", "-c", "print(42)"]), tracked=False)
   run.run()

The transport owns local invocation versus SSH RPC, including streaming
FOLLOW. Both targets use the same backend methods. An SSH application
error is decoded separately from a broken transport; a confirmed
``SubmissionNotStarted`` remains a confirmed non-submission across SSH.
An interrupted transport remains uncertain and is never retried as a new job.

Transports live under ``kkrunner.targets``. Their concrete implementations
are organized symmetrically in ``implementations/local/`` and
``implementations/ssh/``, each with a ``target.py`` and ``call.py``.
Shared transport contracts and invocation helpers remain in the parent package.
``LocalTarget`` and
``SshTarget`` combine immutable connection settings with the actual calls;
there is no separate endpoint type. ``ExecutionMetadata.target`` stores the target,
whose dataclass fields serialize to JSON, including an explicit ``kind`` tag.
``Target.to_dict()`` exposes the same connection settings.
``ExecutionClient.from_config(target, backend)`` reconstructs supported clients from
objects or JSON dictionaries. Only local/ssh targets and the backend names
direct/kkrunner/psij are allowed; records never contain arbitrary Python imports.
Persistent unsupported custom targets are rejected before a Run is created.
ClientPool caches clients by transport/backend pair; discarding a client or
clearing the cache does not close connections. The SSH transport reuses a
bounded pool of persistent SSH/Python sessions for short RPC calls, shared
by all clients with equal SSH configurations. Each idle session expires at
its own deadline, managed by one process-wide sleeping timeout thread.
Empty pools and their executors are removed automatically.
Explicit ``SshTarget.close()`` closes that configuration's shared sessions;
it is not a client shutdown operation. Streaming operations and file
transfers keep their own processes and stream handles.

A single FileSession handles local and remote files. It owns live client file
handles and finalizes the sole FileManifest. Local execution needs no copying;
SSH execution stages inputs, relays live output only to observers and
synchronizes all declared result files (including stdout/stderr) after native
completion. The local copies are not written during observation.
Managed backends wait for terminal status without following stdout/stderr
unless an output listener is registered. An initial supervised attached
connection can own the execution lifetime without subscribing to output.
Use ``add_output_listener(callback)`` and ``remove_output_listener(callback)``
on a Job or Run to change its listeners. Adding the first or removing the
last listener changes the output subscription on the existing connection.
Removing an absent listener is harmless; a callback already in progress
may finish. Waiting for native completion continues independently.
Named streams resume from their previous byte cursor; live-only bytes
emitted without a subscriber cannot be recovered.
Each new calculation creates fresh named output artifacts. A previous
reader holding an open file descriptor can continue reading its original
file after the pathname has been reused. A file observer opens a stream
on demand and retains that descriptor across subscription pauses until
observation ends; it does not read or watch the file while unsubscribed.
Already-open handles supplied by the caller retain their existing semantics. A late subscription that
crosses the terminal reply completes its named-stream replay without another
backend status query, but only if the existing terminal message does not
confirm that the output subscription was drained.
Only Direct attached convenience calls display unredirected console
streams automatically. For managed and detached Direct calls, an explicit
output listener requests live output.
Direct attached retains the native stream semantics of a session-owned
process. Direct detached writes its named files independently; the same
portable backend tails those files for local and SSH calls only when a
listener requests output, including during an already running execution. After
staging, transfer selections and cache baselines come from the manifest rather
than independently maintained lists. Snapshots and manual retrieval are in the
file-access layer, with the client delegating those requests.

``Job.state`` and ``Job.status()`` expose cached native facts without I/O.
``Job.refresh_status()`` asks the backend binding for its status, using an
independent query when supported or the live call's last facts otherwise.
``Run.refresh()`` refreshes and returns the current RunState, whether or not
the Run is tracked. Use ``run.record`` for the optional durable record.
A terminal native status while output observation is active means FINISHING;
the observer still owns completion. After an untracked observer has stopped,
refresh reports the backend's current progress, or DOWNLOADING if execution
has ended and result collection is still pending. It does not retrieve
results, resume observation or invoke postprocessing.
Job identities are opaque backend references; Job never reads or serializes
a PID. STATUS, REATTACH and RECOVER are explicit capabilities. Recording a
direct process identity allows output-only reattachment from saved files,
not resubmission or recovery of the original exit code. Linux identities also
support verified cancellation after restart. The direct backend can query that
process independently over local or SSH transport.
When a vanished process is verified, its run can become FINISHED with
``return_code=None``; this releases file reservations, but does not establish
successful execution or authorize success-only postprocessing.

FileContext owns the shared projection of paths and manifests. A symbolic
directory preview performs no target I/O; reservation resolves the same mapping
without staging or opening output files. FileSession adds transfer results and
cache baselines after staging. Run applies the resolved directory and manifest
to its record before RunManager reserves it; ExecutionClient never rewrites a
RunRecord. Native status replies use one decoder before projection into client
RunStatus values.

Tracking is opt-in for every backend: ``tracked=True`` uses the default
RunManager, and ``tracked=manager`` selects a specific manager. The default
``tracked=False`` creates no client run record. Backend identities and
reattachment capabilities are independent of this choice. An optional
``preprocess`` callback on ``create_run()`` runs before staging and submission
whether or not tracking is enabled. Tracked runs reserve the declared
writable input and output paths before preprocessing. Untracked runs do not
take these reservations; use a manager backed by ``MemoryRunStore`` for
conflict protection without disk history.

Execution intent and staging
----------------------------

``ExecutionSpec.mpi`` is a :class:`~kkrunner.execution.MpiSpec`. File mapping
and synchronization are a separate :class:`~kkrunner.client.StagingSpec` in
``ExecutionSpec.staging``. For example:

.. code-block:: python

   from kkrunner import ExecutionSpec
   from kkrunner.client import StagingSpec
   from kkrunner.execution import MpiSpec

   spec = ExecutionSpec(
       ["serial-program"],
       mpi_argv=["mpi-program"],
       mpi=MpiSpec.from_value(4),
       directory="/home/user/work/run",
       staging=StagingSpec(local_root="/home/user/work", remote_root="/scratch/runs"),
   )

The client projects this declaration to an ``ExecutionRequest`` with target
paths and MPI options only. Backend code never reconstructs a client
``ExecutionSpec`` and never receives local roots or synchronization policy.
Shared state/MPI values live in ``kkrunner.execution`` without importing the
client, targets, file manifests or run monitoring. There is no second native
specification object alongside the JSON request.

``StagingSpec.filesystem_id`` identifies the target storage namespace for file
reservations and is persisted with ``FileMapping``. It does not belong to SSH
connection settings: one connection can reach multiple filesystems and several
connections can reach the same storage. Local copying onto the same file,
including a hardlink or symlink alias, preserves that file without replacing it.
