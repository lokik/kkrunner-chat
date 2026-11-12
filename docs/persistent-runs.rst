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
Use ``manager.create_run(client, spec)`` and ``run.submit()`` to start one.
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
   from kkrunner.client import EndpointClient

   workspace = tempfile.TemporaryDirectory()
   directory = Path(workspace.name)
   manager = RunManager(RunStore(directory / "runs"))
   run = manager.create_run(
       EndpointClient(SupervisedBackend),
       ExecutionSpec(
           [sys.executable, "-c", "print(43)"],
           directory=directory,
           output_file="stdout.txt",
       ),
       processing_required=True,
   )
   try:
       run.run()
   except ProcessingPending:
       print(manager.store.get(run.id).state.value)

   # Recreate the client; the processor is application-owned and not persisted.
   resumed = RunManager(RunStore(directory / "runs"))
   attached = resumed.attach(
       run.id,
       process_result=lambda outcome: int(Path(outcome.output_file).read_text()),
   )
   print(attached.wait())
   print(resumed.store.get(run.id).state.value)

.. testoutput::

   postprocessing
   43
   finished

.. testcleanup::

   workspace.cleanup()

Processing ownership uses an OS lock and a persisted attempt token. Observing
or refreshing a record does not take ownership. A crash can cause a later
explicit retry to repeat application side effects, so processors should be
idempotent or use their own transactions. Exactly-once external side effects
are not guaranteed. The returned Python object is not serialized in the record;
applications reconstruct historical results from metadata and verified files.

Lifecycle and recovery decisions
----------------------------------

The normal sequence is ``new -> submitting -> queued/running -> downloading``
when needed, then ``postprocessing`` when required, then ``finished``.
``failed`` and ``cancelled`` are terminal. ``unknown`` means observation is
uncertain; it is not evidence that the backend stopped.

* ``run.stop_observation()`` detaches this observer without cancelling execution.
* ``run.cancel()`` requests backend cancellation.
* ``manager.refresh()`` reconciles backend facts and durable terminal receipts.
* ``manager.attach(id, process_result=...)`` resumes a nonterminal run.
* ``manager.resume_download(id)`` reopens a recoverable failed download; attach
  afterward to resume transfer and completion.
* ``manager.retry_postprocessing(id, processor)`` retries failed/interrupted
  processing without submitting the command again.

Do not attach a terminal record. Load its historical application result, or use
the explicit retry operation for a supported failure phase. A recoverable failed
download retains its backend outcome receipt until retry settles. Other terminal
outcomes are acknowledged after the durable state is committed; refresh can
retry a lost acknowledgement.

Recovery after a crash during submission depends on the backend. See
:ref:`backend-recovery` for the distinction between reconnecting with a known
backend ID and discovering a submission whose ID was never saved.

Store and metadata contracts
------------------------------

``RunStore.save`` creates a globally unique ID or repeats an identical save.
Use its atomic update API for subsequent changes. A date shard does not create
a separate ID namespace. Invalid or duplicate records raise
:class:`~kkrunner.runs.CorruptRunRecord`; they are not silently ignored.

Metadata is namespaced JSON data, recursively immutable after record creation.
It cannot change run identity, choose parsers, or execute code. Put the
application's reconstruction parameters in its namespace and keep executable
callbacks in the application process.
