# Execution boundary and public contract

KKRunner owns ExecutionSpec/ExecutionOutcome, TaskFile, EndpointClient,
RunState/FailurePhase, ExecutionMetadata/WorkingDirectory, RunRecord/RunStore,
FileEntry/FileMapping/FileManifest, FileState/FileVersion/FileFingerprint,
ExecutionResources. The root `kkrunner` package exports
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
`FileSession` owns staging, output persistence, synchronization and
verification. The client creates these parts through `RunParts`;
`RunManager` owns persisted lifecycle transitions and `RunFiles` updates file
availability in the same store. A terminal backend fact must never
be published directly as a terminal client Run before these phases settle.

Backend facts and durable submission/outcome receipts are not a second run
record. RunRecord remains the authority for end-to-end completion. All completion paths share the same durable commit and result ACK.
RunManager.refresh also reconciles terminal receipts; the explicit
reconcile_acknowledgements() method retries lost ACKs without changing outcomes.
Recoverable download failures retain their backend outcome receipt until retry
finishes; otherwise a later attach would lose authoritative backend completion. JSON writes use atomic replacement, file and directory
fsync on POSIX; portable Windows directory durability is weaker as documented
in `kkrunner.support.durability`. Invalid records raise errors, including during enumeration.

Run accepts an explicit owned_resources=ExecutionResources(...) argument for
additional application resources. It releases them after output finalization,
postprocessing and durable completion, or when launch is definitively prevented.
The same lifecycle handles cancellation during staging and submission; there is
no outer runner proxy. FileSession retains ownership of its streams and closes
them before postprocessing. Additional owned resources remain available through
postprocessing and survive uncertain execution or pending processing. An unstarted
Run can be closed explicitly; closing prevents later submission. The resource
ownership is process-local and is not stored in RunRecord.
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
projects; untracked `Run.untracked()` executions do not take these reservations.

Before reservation the SSH client resolves absolute target paths without
staging or opening outputs. Only after the complete footprint has been durably
reserved may uploads and submission begin. Reservations check local read/write
paths (including uploads and downloaded/streamed results) independently of the
backend, and check the target filesystem as well. Local process and local PSI/J
therefore share the local filesystem namespace. SSH connections to the same
host share a target namespace regardless of SSH flags, remote command or backend.

For host aliases or shared mounts, pass filesystem_id to StagingSpec. The ID belongs to the run
file mapping, independently of its connection. Matching IDs declare one shared target filesystem; distinct
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

`support.process_identity.ProcessIdentity` owns process identity and uses
public psutil API for process facts in native supervisor handles. Such a handle stores the PID, creation time,
time basis and, on Linux, the kernel boot ID. On Linux the creation time is normalized as
`Process(pid).create_time() - psutil.boot_time()`. Each observation creates a
fresh `Process` because its creation time is cached. Boot-time reads before and
after the observation detect a clock adjustment; an inconsistent observation is
retried at most three times. A 1 microsecond absolute tolerance, with no relative
tolerance, covers floating-point subtraction without accepting a neighboring
Linux clock tick. No old tick-based handle is interpreted as the new format.

Elsewhere the identity uses the absolute `create_time()`. Its precision and
behavior under clock adjustments depend on the platform. PID plus creation time
is not a cryptographic or globally unique identity. Linux also requires an
identical boot ID, so an identity from another boot cannot establish a match.
A Linux identity without boot information is unverifiable. Other platforms
retain their public psutil timestamp guarantees. psutil's public `is_running()` checks for PID reuse during observation;
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
| `kkrunner.client` | EndpointClient, backend composition, client pools, transports, Job and FileSession. |
| `kkrunner.remote` | Stateless target protocol service and framed stream delivery; no client execution facade. |
| `kkrunner.projects` | Project repository, project-bound manager and explicit project-manager resolution. |
| `kkrunner.files` | TaskFile, manifests, transfer policy, pathname declarations, captured versions and artifact availability. |
| `kkrunner.support` | Shared durability, locking, asynchronous and growing-stream utilities. |

The root package exports eight backend-independent entry points:
`ExecutionSpec`, `ExecutionOutcome`, `Run`, `RunManager`, `RunStore`,
`RunState`, `TaskFile` and `ProcessingPending`. Select a backend class
(`DirectBackend`, `SupervisedBackend`, `PsijBackend`) from `kkrunner.backends`
and supply it to `EndpointClient`. Configured backend instances hold immutable
options and capabilities; their classes expose the static target operations.

The three classes declare static target operations. An EndpointClient retains
the configured backend instance; persisted BackendConfig uses a whitelisted
backend name and options to reconstruct it. Client Jobs, staging, file synchronization and attachment
belong to `kkrunner.client`.

`backends.support.class_helper.class_helper` builds an import-only namespace with
matching static methods, preserving synchronous/asynchronous methods. Each
backend independently chooses its `SshHelper`: direct uses itself, supervised
and PSI/J use the generated namespace. SSH transmits **one selected method**,
not a whole class. The installed helper imports and calls the original method.
Helper selection does not determine persistence or reattachment capability.

`DirectBackend.stream` is one self-contained asynchronous implementation. Its
local invocation receives explicit streams and control; its SSH invocation
receives framed sinks from the remote invocation bootstrap. Only the target
operation starts the calculation, terminates its process group and interprets
its outcome. It publishes acceptance with an opaque session identity and
returns native status, including confirmed cancellation and launch failures.
`Job` uses the `BackendExecution` interface for submit, observe, status and
cancel, without inspecting PID, exit codes or backend execution modes.
`client.backend_execution` binds those operations to the selected transport;
session-bound and managed semantics are selected once at composition.
`client.transports.calls` owns local coroutine/control resources or the SSH
command and framed delivery. A calculation's exit code is payload data,
independent of the SSH command's exit status. No additional local interpreter
or worker thread runs the direct backend.

Native supervisor and scheduler internals live in `backends/supervised` and
`backends/psij`, split into worker, streams, receipts, submission and scheduler
operations. The supervisor remains an independent process with its existing
release barrier. Its output pump publishes synchronously to LIVE followers;
per-follower asynchronous REPLAY catches up from the spool and returns to LIVE
without yielding between the EOF check and mode switch.

Local managed FOLLOW awaits the existing backend coroutine. SSH runs a Python
follower that invokes that coroutine and sends framed calculation output and
terminal facts; transport stderr carries diagnostics. `client.transports.follow`
owns reading and reaping that SSH follower. `Job.stop_observation()` cancels
only its observation task. The supervisor/calculation survives; `Job.cancel()`
invokes the separate backend cancellation operation. PSI/J uses native status
callbacks and growing-file observation, unregistering callbacks on detach.

Generic file operations and shared support do not own the run lifecycle.
Shared execution vocabulary lives in kkrunner.execution without client
imports. ExecutionSpec and StagingSpec are client declarations; backends consume
only the target ExecutionRequest with MPI options and target paths. File reservations
use generic path matching and the persisted transport/mapping namespace.
The manifest and execution identity are separate models.
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
transport or its `EndpointClient`, including custom Python environments.

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

## Backend identity, client attachments and Run clients

A backend accepts a serializable execution request or an opaque job handle.
The same handle is used locally and over SSH, including PSI/J; the client does
not unpack it into a scheduler ID and reconstruct it from transport options.
A client `Job` may expose `JobRef(backend.name, handle)` without interpreting
the handle. Native scheduler IDs remain native backend facts.

`JobAttachment` and `SubmissionContext` are client snapshots of persisted
execution and file declarations. Clients project the target request and
stream paths; native operations do not receive a `RunRecord` or `FileManifest`.
Only a `EndpointClient` creates or reopens `RunParts` (a Job plus file resources).
Backends do not manufacture client Jobs.

`BackendCapability.REATTACH` explicitly describes native reattachment. Manager
checks this capability rather than inferring support from the presence of a
method. Recovery returns `RecoveryResult` with found/absent/indeterminate/
unsupported semantics; having an identity does not imply recovery support.
Direct execution cannot reconnect. Supervisor recovery needs retained receipts;
PSI/J recovery depends on the configured scheduler. Unsupported or uncertain
recovery never authorizes a duplicate submission.

| Role | Implementations |
| --- | --- |
| Static target operations | `DirectBackend`, `SupervisedBackend`, `PsijBackend` |
| Client lifecycle, cached backend status, result and listeners | `client.job.Job` |
| Local and SSH session composition | `EndpointClient` |
| Path previews and resolved manifest projection | `client.file_context.FileContext` |
| Backend execution semantics bound to a transport | `client.backend_execution`, `client.contracts.BackendExecution` |
| Live invocation/control resources and transport diagnostics | `client.transports.calls` |
| Managed output delivery and SSH follower cleanup | `client.transports.follow` |
| Immutable connection settings, calls and file transport | `client.transports.LocalTransport`, `client.transports.SshTransport` |
| Persistence and end-to-end completion | `RunManager`, `Run`, `RunStore` |
| Reuse of configured clients | `ClientPool` |

The backend contract and pure identity/status/recovery values live in
`backends.contracts`. Client Job/composition protocols and attachment contexts
live in `client.contracts`. `Operation` in that module describes which backend
method or standalone function to call. `client.transports.rpc` owns the
JSON/streaming bootstrap, argument encoding and reply/error decoding together.
`client.transports.source` checks
and extracts the selected function's source when building a remote command;
local operation references do not require source delivery.
`backends.contracts.decode_job_status` decodes native replies for both live
backend bindings and endpoint-wide queries. `RunStatus.from_job_status` only
adds client context to already-decoded backend facts.
Local short calls invoke the selected method directly; asynchronous methods
run in the caller's event loop. Synchronous entry points use `asyncio.run`
only at an independent interpreter/CLI boundary.

There is one client Job across direct, supervisor and scheduler modes. Native
PSI/J objects serve a single target operation; they do not duplicate Job/Run
lifecycle state. There is no second local direct implementation, legacy native
PSI/J client Job, or process-based local FOLLOW fallback.


## Orthogonal location and backend selection

`EndpointClient(BackendClass, transport=...)` is the sole standard execution client.
LocalTransport calls the selected static operation directly; SshTransport delivers
its actual portable method or import-only helper. ExecutionMetadata.transport holds the
immutable LocalTransport or SshTransport under client/transports; these serialize
their connection settings directly. ExecutionMetadata.backend is a separate BackendConfig. Persistent custom transports without a supported reconstruction
are rejected before creating a Run.

Every Run has one FileSession. It stages files when needed, owns client streams,
and finalizes the FileManifest after native execution. Local finalization does
not copy files. Transfer indexes are derived from the staged manifest; temporary
staging data is discarded when ownership moves to that manifest.

RunManager composes a RunFiles service with the same RunStore and client pool.
The service checks captured versions, reads live snapshots, restores missing
historical files with digest verification, and persists artifact availability.
It neither changes run lifecycle state nor calls back into RunManager. Stateless
manifest capture and file checks are module functions in runs/files.py;
RunManager retains its public file methods as explicit delegates.

A direct execution receives an opaque session identity from its backend;
the client Job never reads or serializes a PID. STATUS, REATTACH and RECOVER capabilities are independent
of having an identity. Losing a direct observer does not provide an exit code
or prove successful completion.
