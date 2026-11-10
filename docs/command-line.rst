Command-line interfaces
=======================

The package installs two separate commands:

* ``kkrunner`` is a client that reads a local ``RunStore`` and selects runs
  by their persistent ``run_id``. It does not submit or supervise jobs.
* ``kkrunner-agent`` runs one control request on the execution host and
  exits. It is not a daemon and does not persist client run records. It
  understands opaque backend handles, never ``RunStore`` selection.

Client commands
---------------

.. code-block:: console

   kkrunner --store /path/to/runs status RUN_ID
   kkrunner --store /path/to/runs follow RUN_ID --from-start
   kkrunner --store /path/to/runs cancel RUN_ID

For a direct backend handle (which lacks the metadata, download state, and
postprocessing ownership of a persisted run), explicitly use ``--remote-id``.
These commands run locally if ``--host`` is omitted, or send control requests
over SSH when a host is supplied:

.. code-block:: console

   kkrunner status --remote-id HANDLE
   kkrunner --host user@cluster status --remote-id HANDLE
   kkrunner --host user@cluster follow --remote-id HANDLE --from-start
   kkrunner --host user@cluster cancel --remote-id HANDLE

A handle alone cannot identify a ``RunStore``, application result, or
a stream file location. For scheduler handles, direct follow may require
``--stdout-path`` and ``--stderr-path``. Run-ID operations use recorded
endpoint and stream paths instead.

Agent protocol
--------------

The agent is invoked once per client request, normally through SSH. Every
internal operation requires an explicit protocol version. For example:

.. code-block:: console

   kkrunner-agent --protocol-version VERSION status HANDLE
   kkrunner-agent --protocol-version VERSION submit DIRECTORY
   kkrunner-agent --protocol-version VERSION query-files

Additional internal operations are ``follow``, ``cancel``, ``recover``,
``ack-submit``, ``ack``, ``resolve-mpi`` and ``finalize-upload``.
Submission and recovery read JSON requests from stdin; status and acknowledgments
return JSON. The framed follow protocol carries both output channels and terminal
status. Human-facing clients should use ``kkrunner`` or the Python API,
not construct agent protocol messages manually.

Help and parser construction for both commands remain lightweight. The former
``kkrunner_cli.user_parser`` bridge is removed; applications may provide
their own CLI over KKRunner's backend and persistence APIs.
