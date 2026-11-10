"""One client execution session: a Job and optional file-transfer ownership.

Run delegates transport-specific preparation here. No backend execution logic
lives in this facade; Job remains the only owner of process state.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    from kkrunner.backends.contracts import Job
    from kkrunner.client.remote_files import RemoteFileSession


@dataclass
class RunParts:
    """Constructed together and handed over to a Run as one resource unit."""

    job: "Job"
    files: Optional["RemoteFileSession"] = None
    submission_builder: Optional[Callable[[dict], dict]] = field(default=None, repr=False)
    _claimed: bool = field(default=False, init=False, repr=False)

    def prepare_submission(self):
        """Persist file staging information before the irreversible submit."""
        if self.files is not None:
            self.files.prepare_submission()
            return True
        prepare = getattr(self.job, "prepare_submission", None)
        if prepare is not None:
            prepare()
            return True
        return False

    def prepare_job(self):
        """Wire a staged client file session into its execution mechanism."""
        if self.files is None:
            return
        from kkrunner.runs.base import SubmissionNotStarted
        try:
            prepared = self.files.prepare_job()
            request = self.submission_builder(prepared) if self.submission_builder is not None else prepared
            self.job.configure_submission(request, **self.files.observation())
        except SubmissionNotStarted:
            raise
        except Exception as exc:
            # The execution mechanism has not been submitted yet.
            raise SubmissionNotStarted(str(exc)) from exc

    def configure_observation(self):
        if self.files is not None:
            self.job.configure_observation(**self.files.observation())

    def claim(self):
        """The Run completion observer now owns eventual file closure."""
        self._claimed = True

    def abort(self):
        """Close a prepared but unclaimed attachment when opening fails."""
        if not self._claimed and self.files is not None:
            self.files.close()

    def close(self):
        if self.files is not None:
            self.files.close()


from typing import Protocol, runtime_checkable


@runtime_checkable
class RunEndpoint(Protocol):
    """Client-side factory; direct endpoints need not support reattachment."""

    def create_run_parts(self, spec, *, submission_key=None, callback=None) -> RunParts: ...


@runtime_checkable
class ReopenableRunEndpoint(RunEndpoint, Protocol):
    """Optional capability for opening a client session around an existing Job."""

    def open_run_parts(self, attachment, *, callback=None, output_listener=None) -> RunParts: ...


class NativeRunEndpoint:
    """Compose a native Backend into the client-side Run protocol.

    The native backend never needs to import RunParts or know how the client
    stores and synchronizes files.
    """

    def __init__(self, backend):
        self.backend = backend

    def create_run_parts(self, spec, *, submission_key=None, callback=None):
        return RunParts(self.backend.create_job(spec, submission_key=submission_key, callback=callback))

    def open_run_parts(self, attachment, *, callback=None, output_listener=None):
        return RunParts(self.backend.open_job(
            attachment, callback=callback, output_listener=output_listener, start=False,
        ))


def run_endpoint_for_backend(backend) -> RunEndpoint:
    """Native mechanisms require a client composition; remote proxies are endpoints."""
    from kkrunner.backends.contracts import Backend

    if isinstance(backend, Backend):
        return NativeRunEndpoint(backend)
    if not isinstance(backend, RunEndpoint):
        raise TypeError("Expected a native Backend or a client RunEndpoint")
    return backend
