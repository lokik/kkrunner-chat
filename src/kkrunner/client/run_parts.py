"""One client execution session: a Job and optional file-transfer ownership.

Run delegates transport-specific preparation here. No backend execution logic
lives in this facade; Job remains the only owner of process state.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    from kkrunner.client.job import Job
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
        prepare = None if getattr(self.job, "_direct", False) else getattr(self.job, "prepare_submission", None)
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

    def configure_detached(self, detached, *, default=False):
        """Configure process lifetime only when the mechanism supports the choice."""
        if type(detached) is not bool:
            raise TypeError("detached must be bool")
        configure = getattr(self.job, "configure_detached", None)
        if getattr(self.job, "_direct", None) is False:
            # The shared Job class also represents managed calculations.
            # For them detached execution is the backend's fixed policy,
            # not a configurable direct process lifetime.
            configure = None
        if configure is not None:
            configure(detached)
            if self.files is not None:
                # Only direct SSH has a configurable Job and remote file
                # session. Agent/PSI-J continue following their own streams.
                self.files.configure_direct_detached_streams(detached)
        elif detached != default:
            raise ValueError("This backend does not support changing its detached policy")

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
