Client, connection and backend
==============================

``EndpointClient`` provides access to a configured backend through a transport.
It can serve multiple runs and composes their ``Job``, backend binding and
``FileSession``. There is one client implementation for local and SSH execution:

.. code-block:: python

   from kkrunner import ExecutionSpec, Run
   from kkrunner.backends import DirectBackend, PsijBackend
   from kkrunner.client import EndpointClient, SshTransport

   local = EndpointClient(DirectBackend)
   remote = EndpointClient(
       PsijBackend(executor_name="slurm"),
       transport=SshTransport("user@cluster"),
   )
   run = Run.untracked(local, ExecutionSpec(["python3", "-c", "print(42)"]))
   run.run()

The transport owns local invocation versus SSH RPC, including streaming
FOLLOW. Both transports use the same backend methods. An SSH application
error is decoded separately from a broken transport; a confirmed
``SubmissionNotStarted`` remains a confirmed non-submission across SSH.
An interrupted transport remains uncertain and is never retried as a new job.

Transports live under ``kkrunner.client.transports``. ``LocalTransport`` and
``SshTransport`` combine immutable connection settings with the actual calls;
there is no separate endpoint type. ``ExecutionMetadata.transport`` stores the transport,
whose dataclass fields serialize to JSON, including an explicit ``kind`` tag.
``Transport.to_dict()`` exposes the same connection settings.
``EndpointClient.from_config(transport, backend)`` reconstructs supported clients from
objects or JSON dictionaries. Only local/ssh transports and the backend names
direct/process/psij are allowed; records never contain arbitrary Python imports.
Persistent unsupported custom transports are rejected before a Run is created.
ClientPool uses the transport/backend pair as its identity. Transports own no
persistent open connection: each operation owns its process and stream handles.

A single FileSession handles local and remote files. It owns live client file
handles and finalizes the sole FileManifest. Local execution needs no copying;
SSH execution stages inputs, relays streams and retrieves results. After
staging, transfer selections and cache baselines come from the manifest rather
than independently maintained lists. Snapshots and manual retrieval are in the
file-access layer, with the client delegating those requests.

``Job.state`` and ``Job.status()`` expose cached native facts without I/O.
``Job.refresh_status()`` asks the backend binding for its status, using an
independent query when supported or the live call's last facts otherwise.
Job identities are opaque backend references; Job never reads or serializes
a PID. STATUS, REATTACH and RECOVER are explicit capabilities. Recording a
direct session identity does not provide reattachment.
An exited process whose observer was lost has no recoverable return code and
must not be reported as a successful completed calculation.

FileContext owns the shared projection of paths and manifests. A symbolic
directory preview performs no target I/O; reservation resolves the same mapping
without staging or opening output files. FileSession adds transfer results and
cache baselines after staging. Run applies the resolved directory and manifest
to its record before RunManager reserves it; EndpointClient never rewrites a
RunRecord. Native status replies use one decoder before projection into client
RunStatus values.

``Run.untracked`` selects absence of persistence, independently of backend.
``Run.create(..., persistent=...)`` selects tracking policy. The configured
client supplies the default tracking policy; native backends do not own it.

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
client, transports, file manifests or run monitoring. There is no second native
specification object alongside the JSON request.

``StagingSpec.filesystem_id`` identifies the target storage namespace for file
reservations and is persisted with ``FileMapping``. It does not belong to SSH
connection settings: one connection can reach multiple filesystems and several
connections can reach the same storage. Local copying onto the same file,
including a hardlink or symlink alias, preserves that file without replacing it.
