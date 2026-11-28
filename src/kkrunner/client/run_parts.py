"""One client execution session: a Job and its file-transfer ownership.

Run delegates transport-specific preparation here. No backend execution logic
lives in this facade; Job observes the selected backend's execution state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kkrunner.client.file_session import FileSession
    from kkrunner.client.contracts import JobProtocol


@dataclass
class RunParts:
    """Constructed together and handed over to a Run as one resource unit."""

    job: "JobProtocol"
    files: "FileSession"

    def prepare_job(self):
        """Wire a staged client file session into its execution mechanism."""
        from kkrunner.runs.base import SubmissionNotStarted
        try:
            self.job.configure_submission(self.files.prepare_job(), **self.files.observation())
        except SubmissionNotStarted:
            raise
        except Exception as exc:
            # The execution mechanism has not been submitted yet.
            raise SubmissionNotStarted(str(exc)) from exc

    def configure_detached(self, detached):
        self.job.configure_detached(detached)

    def configure_observation(self):
        self.job.configure_observation(**self.files.observation())

    def close(self):
        """Release the file resources of this execution bundle."""
        self.files.close()
