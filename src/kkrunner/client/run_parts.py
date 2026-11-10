"""One client execution session: a Job and optional file-transfer ownership.

Run delegates transport-specific preparation here. No backend execution logic
lives in this facade; Job remains the only owner of process state.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from kkrunner.backends.contracts import Job
    from kkrunner.client.remote_files import RemoteFileSession


@dataclass
class RunParts:
    """Constructed together and handed over to a Run as one resource unit."""

    job: "Job"
    files: Optional["RemoteFileSession"] = None
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
            request = self.files.prepare_job()
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


# One interface authority: the backend's public contract builds RunParts.
from kkrunner.backends.contracts import Backend as RunEndpoint
