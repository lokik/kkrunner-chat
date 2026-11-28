Persistent runs and recovery
==============================

A calculation can keep running after its controlling Python client exits.
With a supported backend, KKRunner can reconnect to that existing process or
scheduler job and continue monitoring, file downloads and result processing.
Reconnecting does not restart a process that has been killed. Keep the run
store, working files and backend service available; :ref:`backend-recovery`
describes the requirements for each backend.

A **run** includes the command execution and any transfers or application
processing needed to obtain its result. A scheduler **job** is the execution
part of that run. The backend process may have finished while the run is still
downloading files or processing results.

:class:`~kkrunner.RunStore` stores durable :class:`~kkrunner.runs.RunRecord` objects
in a directory. Each record contains a stable run ID, backend identity, file
manifest and lifecycle state. :class:`~kkrunner.RunManager` creates managed
runs backed by that store and returns a :class:`~kkrunner.Run`.
Tracking is opt-in for every backend: ``tracked=True`` uses the default
manager, while ``tracked=manager`` selects a specific one. Without a run
record, backend handles and backend-level observation remain independent of
client history; see :doc:`command-line`.
Use ``Run.create(spec, backend='kkrunner', tracked=manager)`` and
``run.submit()`` to start one.
After restarting the client, reopen the same store and call
``manager.attach(run_id)`` to continue a nonterminal run.

Projects optionally group these runs by name and working directory.
:class:`~kkrunner.projects.ProjectRunManager` adds project membership to the same
lifecycle; persistence does not require a project. See :doc:`projects`.

Result processing
-------------------

``process_result`` consumes an :class:`~kkrunner.ExecutionOutcome` and returns
an application result. It is separate from the completion-notification callback.
``processing_required=True`` allows supplying the processor after reconnect.
If the processor is unavailable or another client owns/performed processing,
``wait()`` raises :class:`~kkrunner.ProcessingPending`; it must not substitute a
raw outcome for the application's result type.

This example lets the command finish, then recreates the client and supplies
its missing result processor. The command is not submitted again:

.. testcode::

   import sys
   import tempfile
   from pathlib import Path
   from kkrunner import ExecutionSpec, ProcessingPending, RunManager, RunStore
   from kkrunner.backends import SupervisedBackend
   from kkrunner.client import ExecutionClient

   workspace = tempfile.TemporaryDirectory()
   directory = Path(workspace.name)
   manager = RunManager(RunStore(directory / "runs"))
   run = ExecutionClient(SupervisedBackend).create_run(
       ExecutionSpec(
           [sys.executable, "-c", "print(43)"],
           directory=directory,
           output_file="stdout.txt",
       ), tracked=manager,
       processing_required=True,
   )
   try:
       run.run()
   except ProcessingPending:
       print(manager.store.get(run.id).file_state.value)

   # Recreate the client; the processor is application-owned and not persisted.
   resumed = RunManager(RunStore(directory / "runs"))
   print(resumed.retry_postprocessing(
       run.id, lambda outcome: int(Path(outcome.output_file).read_text()),
   ))
   print(resumed.store.get(run.id).file_state.value)

.. testoutput::

   ready
   43
   postprocessed

.. testcleanup::

   workspace.cleanup()

Processing ownership uses an OS lock. Publishing native status does not
require file ownership; collecting and processing files does. A crash can cause a later
explicit retry to repeat application side effects, so processors should be
idempotent or use their own transactions. Exactly-once external side effects
are not guaranteed. The returned Python object is not serialized in the record;
applications reconstruct historical results from metadata and verified files.

Lifecycle and recovery decisions
----------------------------------

The native ``state`` and the client ``file_state`` are independent. Native
execution progresses from ``new`` through submission and running to
``finished``, ``failed`` or ``cancelled``. The file lifecycle progresses
through ``preprocessing``, ``waiting``, ``collecting`` and ``ready``;
processing ends in ``postprocessed`` (with a processor) or ``done``
(without one). File preparation, transfer or processing failures set
``file_state`` to ``failed`` without changing the native outcome. A run
remains active for reservation purposes until its file lifecycle is terminal.
The processor receives the native outcome, including unsuccessful exits.
``failed`` and ``cancelled`` are terminal. ``unknown`` means observation is
uncertain; it is not evidence that the backend stopped.

A verified direct-process termination can have no recoverable OS exit code.
``finished`` with ``return_code=None`` therefore does not assert exit code zero.
An already known exit code is retained when a later observation only establishes
liveness. Application readers may still parse historical OUT files without a
known OS outcome.

Unknown backend names and transport kinds remain readable execution metadata.
Their local files and captured versions can be browsed, but target operations
are unavailable until a supported implementation is supplied. Refresh leaves
these records unchanged, and nonterminal records retain their file reservations.
Malformed records still raise ``CorruptRunRecord`` by default; an explicit
``on_error`` handler may report and skip them in history viewers.

* ``run.stop_observation()`` detaches this observer without cancelling execution.
* ``run.cancel()`` requests backend cancellation.
  ``manager.cancel(id)`` records the native reply without downloading result
  files. Use the existing observer or ``manager.collect_files(id)`` once
  output publication is confirmed.
* ``manager.refresh()`` reconciles backend facts and durable terminal receipts.
* ``manager.attach(id, process_result=...)`` resumes a nonterminal run.
* ``manager.collect_files(id)`` completes confirmed output collection or
  explicitly retries a failed download under one file ownership lock.
  It does not attach to a backend job. If a processor is required, use
  ``manager.retry_postprocessing(id, processor)`` after collection.
* ``manager.retry_postprocessing(id, processor)`` processes ready files or
  retries failed/interrupted processing without constructing an execution
  client or submitting the command again. Receipt cleanup can be reconciled
  separately.

Do not attach a terminal record. Load its historical application result, or use
the explicit retry operation for a supported failure phase. A recoverable failed
download retains its backend outcome receipt until retry settles. Other terminal
outcomes are acknowledged once their durable file snapshot or terminal file
state is committed. A purely local processing retry need not contact the
backend; ``manager.reconcile_acknowledgements()`` can retry a lost acknowledgement.

Recovery after a crash during submission depends on the backend. See
:ref:`backend-recovery` for the distinction between reconnecting with a known
backend ID and discovering a submission whose ID was never saved.

Store and metadata contracts
------------------------------

``RunStore.save`` creates a globally unique ID or repeats an identical save.
Use its atomic update API for subsequent changes. A date shard does not create
a separate ID namespace. Invalid or duplicate records raise
:class:`~kkrunner.runs.CorruptRunRecord`; they are not silently ignored.

Nonterminal records reside in ``active/``; terminal records reside in the
date shard of their creation timestamp. Each run has exactly one JSON
record. Active-only queries and reservation checks do not scan dated
history. Completion publishes the terminal state before moving the record
to history; reopening moves it back before publishing a nonterminal state.
An interrupted move may leave a terminal record in ``active/``; readers
still classify it by its recorded state, not by its location.

Existing date-only stores are migrated once, under the submission lock, on
the first active-only query or write. The migration preserves IDs, contents
and creation dates. It must complete successfully before conflict checks may
skip dated history. Upgrade all clients sharing the store before using the
new layout: older versions do not recognize ``active/``.

Schema 2 is intentionally incompatible with schema 1. Before switching versions,
finish or recover old active runs with their original KKRunner version. Then
archive the old store or explicitly select a new empty store. Active records
from an unsupported schema are not silently ignored during reservation checks:
doing so could allow another calculation to overwrite their files.

Metadata is namespaced JSON data, recursively immutable after record creation.
It cannot change run identity, choose parsers, or execute code. Put the
application's reconstruction parameters in its namespace and keep executable
callbacks in the application process.
