# Execution boundary and public contract

KKRunner owns ExecutionSpec/ExecutionOutcome, TaskFile, ExecutionEndpointConfig,
RunState/FailurePhase, Execution/WorkingDirectory, RunRecord/RunStore,
FileEntry/FileMapping/FileManifest, FileState/FileVersion/FileFingerprint,
ExecutionResources and ResourceOwnedRunner. The root `kkrunner` package exports
the small, backend-independent API; specialized types live in `kkrunner.runs`,
`kkrunner.backends`, `kkrunner.client` and `kkrunner.files`.
`RunManager` tracks runs in an explicit RunStore without requiring a project.
`ProjectRunManager` and ProjectRepository organize runs in projects.
Both managers use the same lifecycle. Each public class has one name that
describes its role.

Applications own input preparation, executable selection, file-template
resolution, parsers, result types and their own metadata namespace. KKRunner
depends on platformdirs and psutil, plus optional psij-python.

TaskFile is an immutable declaration. Its `type` and `info` are opaque strings;
KKRunner does not resolve application templates or consult format registries.
FileEntry is a resolved persistent location plus transfer/publication state.
FileManifest owns the authoritative declarations, cache baselines and captured
versions. `input_file` identifies the main input for tracking and transfer;
`stdin` selects the file connected to the process's standard input.
Read/write conflicts derive exclusively from the manifest; task names
have no locking meaning. Clients must declare every read/write/cache footprint.

RunRecord owns persistent run identity, backend identity, manifest, namespaced
JSON metadata, `processing_required` and the latest processing-attempt ID.
Metadata is recursively immutable and cannot change through RunStore.update.
RunStore.save creates a globally unique ID (or repeats an identical save); all
subsequent changes use its atomic update API. Date sharding does not permit
multiple records with the same ID.
RunRecord schema_version is 1 throughout pre-release development, even when
its structure changes. Only the current shape is accepted. Development records
are not migrated. Earlier record formats are unsupported; copying their JSON
into the current store raises CorruptRunRecord. User configuration
and Project generation/versioning are separate contracts.

Lifecycle: new -> submitting -> queued/running -> downloading (if required) ->
postprocessing (if required) -> finished. Any phase can fail or be cancelled as
supported by the backend. UNKNOWN is recoverable uncertainty, never completion.
Download and processing failures can be explicitly reopened without resubmission.
When processing is required but its callback is unavailable or another observer
owns it, the run remains POSTPROCESSING and wait() raises ProcessingPending.
It never returns a raw outcome as a substitute for the application result.
The callback return object is not persisted; historical applications reconstruct
it from the recorded application metadata and verified files.

Postprocessing claims a per-run filesystem lock, captures/verifies artifacts,
publishes the attempt, calls the processor and commits success/failure before
releasing ownership. Other clients may monitor but cannot process concurrently.
OS lock release after process death makes an interrupted POSTPROCESSING attempt
recoverable. A live owner is never displaced based on a timeout. All clients of
a store must share the same reliable file-locking namespace. Callbacks may be
repeated after a crash and must tolerate that (idempotence or transactional side
effects); exactly-once side effects are not promised.

A backend `Job` owns only execution submission, observation and cancellation.
`JobStatus` describes backend facts, with no downloading or postprocessing.
`JobRef` is an opaque backend-scoped identity rather than a client `run_id`.
`JobAttachment` supplies the execution context needed for reattachment
without exposing a durable record to a backend. An untracked or managed
client `Run` owns execution completion, transfers and optional result processing.
An SSH `Job` owns FOLLOW observation, while its separate
`RemoteFileSession` owns staging, output persistence, synchronization and
verification. An endpoint creates these parts through `RunParts`; only
`RunManager` mutates persistent records. A terminal backend fact must never
be published directly as a terminal client Run before these phases settle.

Backend facts and durable submission/outcome receipts are not a second run
record. RunRecord remains the authority for end-to-end completion. All completion paths share the same durable commit and result ACK.
RunManager.refresh also reconciles terminal receipts; the explicit
reconcile_acknowledgements() method retries lost ACKs without changing outcomes.
Recoverable download failures retain their backend outcome receipt until retry
finishes; otherwise a later attach would lose authoritative backend completion. JSON writes use atomic replacement, file and directory
fsync on POSIX; portable Windows directory durability is weaker as documented
in `kkrunner.support.durability`. Invalid records raise errors, including during enumeration.

Callers own the streams and resources they supply. To transfer their lifetime
to a run, wrap its runner in ResourceOwnedRunner with ExecutionResources. The
wrapper closes owned resources on completion and when submission is cancelled
before launch. Borrowed handles retain their existing ownership semantics.
Managed execution uses file paths on the execution host; the supervisor owns
process lifetime beyond client detach.
Temporary directories must remain available for backend execution and processing;
clients must not delete them on mere loss of observation.

The target-side protocol command is `kkrunner remote`; it rejects incompatible
control protocol versions. A remote host needs KKRunner and the desired
executable, plus PSI/J for scheduler use. Application-specific command wrappers
belong to the application; the protocol implementation remains in KKRunner.

Application-specific preparation and historical result loading stay outside the
runner. Applications retain their own parser and result-format registries and
reconstruct results from namespaced metadata and verified files. They must retain
working directories and artifacts for as long as execution or processing needs
them. See the application guide for its storage policy and temporary-file rules.

## Reservations and filesystems

For runs managed through the same RunStore, submission reserves declared file
paths until the run reaches a terminal state. This applies with or without
projects; untracked `Run.direct()` executions do not take these reservations.

Before reservation the remote executor resolves absolute target paths without
staging or opening outputs. Only after the complete footprint has been durably
reserved may uploads and submission begin. Reservations check local read/write
paths (including uploads and downloaded/streamed results) independently of the
backend, and check the target filesystem as well. Local process and local PSI/J
therefore share the local filesystem namespace. SSH connections to the same
host share a target namespace regardless of SSH flags, remote command or backend.

For host aliases or shared mounts, pass filesystem_id to AgentRunEndpoint
or ExecutionEndpointConfig. Matching IDs declare one shared target filesystem; distinct
IDs declare distinct ones. A client must use the same identity consistently.
The API does not infer that different host aliases refer to the same mount.

Required output files must exist after successful execution. A missing optional
output is skipped; a missing required local output is an ARTIFACTS failure.
A transfer failure remains a DOWNLOAD failure. The backend's return code remains
an execution fact and is not changed to conceal an artifact/processing failure.

## Completion callbacks and platform support

A completion callback may call its own runner's wait() to inspect the result;
this does not wait for the callback itself. Other waiters still await callback
completion and receive its exception. Required processing is a separate callback
from this observation-completion notification.

Managed local execution and the direct-process remote service use POSIX process
groups and Unix sockets. Linux and macOS are covered by CI; Windows-specific
locking/durability code alone does not provide a managed supervisor on Windows.

`support.process_identity.ProcessIdentity` owns process identity and uses only
public psutil API. An opaque backend handle stores the PID, creation time and
time basis. On Linux the creation time is normalized as
`Process(pid).create_time() - psutil.boot_time()`. Each observation creates a
fresh `Process` because its creation time is cached. Boot-time reads before and
after the observation detect a clock adjustment; an inconsistent observation is
retried at most three times. A 1 microsecond absolute tolerance, with no relative
tolerance, covers floating-point subtraction without accepting a neighboring
Linux clock tick. No old tick-based handle is interpreted as the new format.

Elsewhere the identity uses the absolute `create_time()`. Its precision and
behavior under clock adjustments depend on the platform. PID plus creation time
is not a cryptographic or globally unique identity, especially across host
reboots. psutil's public `is_running()` checks for PID reuse during observation;
zombies, vanished processes, access errors and persistently unstable reads never
confirm identity. Without a terminal receipt an unverifiable identity produces
`UNKNOWN`, preserving the run for later recovery. These are non-atomic
observations: a process can exit immediately after a successful check, and a
clock change that reverses entirely between sampled reads can evade detection.

## Package ownership

| Package | Responsibility |
| --- | --- |
| `kkrunner.runs` | Run contracts, execution identity, immutable metadata, durable records, reservations and processing lifecycle. |
| `kkrunner.backends` | Flat direct/supervised/PSI-J functional implementations, common process/MPI support, and backend contracts. |
| `kkrunner.client` | AgentRunEndpoint, connection/endpoint identity, backend pools, transport, remote Job and remote file sessions. |
| `kkrunner.remote` | Stateless target protocol service and framed stream delivery; no client execution facade. |
| `kkrunner.projects` | Project repository, project-bound manager and explicit project-manager resolution. |
| `kkrunner.files` | TaskFile, manifests, transfer policy, pathname declarations, captured versions and artifact availability. |
| `kkrunner.support` | Shared durability, locking, asynchronous and growing-stream utilities. |

The root package exports eight backend-independent entry points:
`ExecutionSpec`, `ExecutionOutcome`, `Run`, `RunManager`, `RunStore`,
`RunState`, `TaskFile` and `ProcessingPending`. Select a local `DirectProcessBackend`
or scheduler `PsijBackend` from `kkrunner.backends`; use
`kkrunner.client.AgentRunEndpoint` with a transport for a remote target.
Projects and advanced file/run types live in their respective subpackages.

The public facades expose the new contracts without deprecated compatibility
classes or parallel names. Internal imports use defining modules. Backend implementations
are loaded only when requested from `kkrunner.backends`: run contracts depend on
endpoint metadata, so loading implementations while initializing that package
would reverse the dependency and create an import cycle. The root import does
not load local/PSI/J implementations, projects or the remote client/service.
The three backend implementations are flat modules: `backends.direct`,
`backends.supervised` and `backends.psij`. Shared process and MPI
primitives live in `backends.support`. Direct local and SSH use the
same direct execution implementation and the private `DirectSession`;
supervised uses its own durable supervisor and FOLLOW; PSI/J uses native
scheduler callbacks and recovery. No backend implementation is hidden
inside a client transport directory or nested backend package.
Target dispatch imports the named module; no plugin registration framework
or long-lived module-global backend instance is required.

`files` and `support` do not import the run lifecycle, projects, backends or
remote service. File reservations use generic local path expansion without
importing a transport. The manifest and execution identity are separate models.
`runs` owns persistence and end-to-end completion; backend implementations report
execution facts. `projects` adds project membership to that existing lifecycle.
A scheduler job is the backend part of a run, while a run also includes staging,
transfer and any required result processing.

## Portable remote file operations

File metadata and discovery use the stateless `kkrunner remote query-files`
command over the same versioned JSON control protocol as job operations. A
request contains `paths`, an optional `root`, and an optional `digest` flag.
Without `root`, paths are literal, including wildcard characters in filenames.
With `root`, paths are root-confined Python glob patterns (`**` is recursive),
with duplicate matches removed. Symlinks are followed for metadata, and discovery
rejects symlinks outside the root. Returned remote paths preserve the requested
namespace, including symlinked roots such as macOS `/tmp`.

Each result contains `path` and `status`: `ok`, `missing`, or `error`. Successful
results include `size`, `mtime_ns` from `st_mtime_ns`, and `type` (`file`,
`directory`, or `other`). Requested SHA-256 digests are computed only for regular
files. Missing exact paths remain explicit; unmatched globs produce no entries.
OS errors retain their errno and message and are raised by the transport rather
than treated as absence. A batch is a sequence of observations, not an atomic
snapshot of the filesystem.

`kkrunner.files.queries` owns these filesystem operations and imports no
transport, backend, or application code. Local and SSH transports share this
implementation. Discovery returns metadata in the same response; cache baselines
query all patterns and exact paths together in one request instead of probing
each match. File queries and execution share the command configured on the SSH
transport or its `AgentRunEndpoint`, including custom Python environments.

Single-file uploads retain temporary-file staging and atomic replacement. The
`finalize-upload` operation uses `os.utime(ns=...)` and `os.replace()` to publish
the temporary file while preserving timestamp precision. Downloads still verify
source signatures before and after copying. No GNU `stat`, `sha256sum`, or
`touch -d` commands are needed. Bulk transfers retain the existing tar pipeline.

There is no remote package-installation bootstrap in KKRunner. Target KKRunner
must already be installed for remote execution and these stateless file
operations. Raw command execution and tar uploads do not invoke the service.
Neither a persistent file server nor an OS-specific fallback is required.

A direct supervisor can fail to launch a process and close its socket before a
new follower receives the protocol greeting. Only an entirely empty greeting,
zero or live-only offsets, and a durable `failed` / `launch` receipt permit
recovery from that race. Partial greetings, truncated frames, invalid offsets,
and malformed protocol data still raise errors. Managed local observation also
preserves protocol validation errors after terminal completion; a receipt does
not make malformed wire data valid. Declared stdout/stderr files are truncated
before launch, so a failed launch cannot expose output from a previous run.

## Independent persistence, detachment and recovery

`Run.create(backend, spec, persistent=None, detached=None)` selects client-side
record storage and process lifetime independently. `persistent=True` uses
`RunManager.get_default()`, while an explicit manager (including a project
manager) controls where the record is stored. `RunManager.as_default(manager)`
selects a scoped default without changing other contexts.

Direct local and direct SSH default to memory-only, attached execution.
Supervised local, agent-backed SSH and PSI/J default to a persisted RunRecord
and backend-independent execution. An explicit false persistence policy does
not disable supervisors, scheduler submission or their backend identities.

A detached direct process outlives the client's connection, but it need not
have a durable backend identity. The target never silently creates stdout or
stderr files; declared file paths receive output directly and undeclared
streams may be discarded. For detached SSH direct execution, the connected observer can still wait,
cancel, retrieve declared streams and complete normal client processing.
Once the client disconnects, the target process survives but its exit code and
client-side synchronization are no longer guaranteed. A recoverable backend
is required for reliable reattachment and completion after client loss. Detached
execution does not provide a second Run lifecycle.

Submission identity belongs to the backend, not the client RunRecord ID.
Recoverable backends retain a submission receipt until the client durably
acknowledges it. Direct backends cannot prove the result of an uncertain
submission after the connection is lost: they preserve UNKNOWN rather than
claiming success or automatically launching again. Cross-process exclusive
postprocessing requires the RunStore coordination available to persisted runs;
a memory-only Run has no cross-client processing lease.

Explicit `detached=False` cannot change a supervisor or scheduler into a
client-owned process: such a backend rejects an unsupported policy rather
than pretending that a lost client will terminate the job.

## Backend identity, client attachments and RunEndpoints

The native `Backend` handles one execution via `create_job()` and (if supported)
`attach_job(BackendJobContext)`. Its backend-only context carries a `JobRef`,
endpoint identity, working directory and explicit stream paths. Native Jobs
never receive a `FileManifest`; the client `RunEndpoint` projects the relevant
paths when reopening a Job.

`JobAttachment` is a **client-side** snapshot of `Execution` and `FileManifest`
used to rebuild a `RemoteFileSession` or to project a native attachment.
`recover_submission(submission_key, SubmissionContext)` takes the original
execution context, because the backend identity is not yet known.

The client `AgentRunEndpoint` and `SshDirectRunEndpoint` assemble `RunParts`
and implement transport-specific staging. They are not native `Backend`
implementations. There are no parallel `*Backend` aliases for these endpoint
classes.
`DirectProcessAdapter` remains an internal mechanism of `Job`, not
a universal layer across PSI/J and supervised execution. `DirectSession`
owns process handles, event loop, cancellation tasks and stream pumping;
only the client `Job` owns the lifecycle state, result and callbacks.

**Contract placement.** The only public `Backend` protocol lives in
`kkrunner.backends.contracts`. It is implemented by native execution
mechanisms (`DirectProcessBackend`, `SupervisedProcessBackend`, `PsijBackend`).
The `RunEndpoint` protocol and the client-only `JobAttachment` live in
`kkrunner.client.contracts`, not in a concrete endpoint implementation.
`DirectProcessAdapter` is private to the direct Job implementation and its
protocol is defined in `kkrunner.backends.direct`.

Local direct and SSH direct use the same `DirectProcessBackend.create_job()`
factory. A transport-specific, private `DirectProcessAdapter` factory supplies
the mechanics; `SshDirectRunEndpoint` owns only session composition and file
transfer. No second SSH-specific public Backend exists.

Direct SSH intentionally has no `open_run_parts()` or
`recover_submission()` method: unsupported capabilities are absent from its
structural protocol rather than advertised as methods that always fail.


### Class responsibilities and naming

| Role | Protocol / type | Implementations |
| --- | --- | --- |
| Native execution | `Backend`, `ReopenableBackend`, `RecoverableBackend` | `DirectProcessBackend`, `SupervisedProcessBackend`, `PsijBackend` |
| Per-client execution handle | `Job` (structural protocol) | `Job`, `Job`, `PsijJob` (internal), `Job` |
| Client session assembly | `RunEndpoint`, `ReopenableRunEndpoint` | `BackendRunEndpoint`, `ReopenableBackendRunEndpoint`, `AgentRunEndpoint`, `SshDirectRunEndpoint` |
| Internal process mechanics | `DirectProcessAdapter` | `LocalProcessAdapter`, `SshProcessAdapter` |
| Command and file transport | `Transport`, `RunningCommand` | `LocalTransport`, `SshTransport`, `SshDirectTransport` |
| Durable execution coordination | `RunManager` | `ProjectRunManager` for project-scoped runs |
| Serializable endpoint descriptor | `ExecutionEndpointConfig` | Distinct from `RunEndpoint`, which constructs sessions |
| Provider instance reuse | `ExecutorPool` | Caches native Backends **or** client RunEndpoints by endpoint descriptor |

All three concrete native Backends explicitly inherit the appropriate protocol.
The supervised process backend is **not** a subclass of the direct process
backend; both can produce a process Job but follow separate execution models.
There is one canonical public name per class, with no `ProcessBackend`,
`RemoteJob` or `BackendPool` aliases.

The `RunningCommand` protocol lives alongside the client-side contracts,
not in `transport.py` next to one of its implementations. The transport base
class remains a shared implementation class, not a second `Backend` protocol.


## One client Job across execution modes

`kkrunner.client.job.Job` is the single client-side lifecycle class.
Managed (supervisor and PSI/J) executions use context-independent operations
(`submit/status/cancel/follow/recover`) available as importable functions.
Local managed and agent SSH use the same `Job` and differ only in how the
endpoint invokes those operations. Direct local and raw SSH use the same
`Job` in session mode, with process/SSH control mechanics implemented by
one private `backends.direct.DirectSession` and transport-specific adapters.
There is no agent-specific, supervised-specific or direct-specific client
Job class. PSI/J's native backend helper still manages PSI/J library objects
internally, not as the public client Job.

A clean direct process session exit is authoritative for direct execution;
a broken channel is not. Managed FOLLOW can lose its connection without
determining target execution outcome, which is checked separately by
`status(JobRef)`. This uses the existing managed FOLLOW protocol and
does not introduce a new framing requirement for direct SSH.
