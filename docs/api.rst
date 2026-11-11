API reference
=============

The root package provides backend-independent Run entry points. Execution
technology lives in :mod:`kkrunner.backends`; SSH transport, endpoint pooling
and remote file synchronization belong to :mod:`kkrunner.client`. The remote
service protocol does not expose a client execution facade.

Common entry points
-------------------

.. currentmodule:: kkrunner

.. autosummary::
   :toctree: _generated

   ExecutionSpec
   ExecutionOutcome
   Run
   RunManager
   RunStore
   RunState
   TaskFile
   ProcessingPending

Execution and durable records
-----------------------------

.. currentmodule:: kkrunner.runs

.. autosummary::
   :toctree: _generated

   LaunchSpec
   MpiSpec
   MpiNotFoundWarning
   RunStatus
   FailurePhase
   Run
   RunRecord
   CorruptRunRecord
   DuplicateSubmissionError
   HistoricalFileMismatchError
   Execution
   WorkingDirectory

Declared files
--------------

.. currentmodule:: kkrunner.files

.. autosummary::
   :toctree: _generated

   FileEntry
   CacheEntry
   FileMapping
   FileManifest
   FileSyncPolicy
   ArtifactAvailabilityState
   FileFingerprint
   FileState
   FileVersion
   FileVersionCaptureError

Backends and their execution contracts
--------------------------------------

.. currentmodule:: kkrunner.backends

.. autosummary::
   :toctree: _generated

   DirectProcessBackend
   PsijBackend
   Job
   JobRef
   JobStatus
   BackendJobContext
   SubmissionContext

.. currentmodule:: kkrunner.backends.contracts

.. autosummary::
   :toctree: _generated

   JobState
   JobFailurePhase
   RecoveryDisposition
   RecoveryResult
   ObservationUnavailable
   UnsupportedCapability

Client endpoints, transport and pooling
---------------------------------------

.. currentmodule:: kkrunner.client

.. autosummary::
   :toctree: _generated

   AgentRunEndpoint
   SshDirectRunEndpoint
   RunEndpoint
   ReopenableRunEndpoint
   JobAttachment
   ExecutionEndpointConfig
   ExecutorPool
   default_executor_pool
   Transport
   LocalTransport
   SshTransport

Projects
--------

.. currentmodule:: kkrunner.projects

.. autosummary::
   :toctree: _generated

   Project
   ProjectRepository
   ProjectRepositoryError
   ProjectConflictError
   ProjectDeleteError
   ProjectRunManager
   default_projects_directory

Implementation interfaces
-------------------------

These modules expose concrete job implementations and resource-ownership
helpers. They are not re-exported by the public facades.

.. currentmodule:: kkrunner

.. autosummary::
   :toctree: _generated

   client.job.Job
   client.job.Job
   backends.psij.PsijJob
   client.remote_files.RemoteFileSession

