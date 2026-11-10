"""Shared lifecycle vocabulary for backend Jobs and client Runs.

JobStatus validates the subset that describes native execution; the Run
may additionally be submitting, downloading or postprocessing.
"""

from enum import Enum


class RunState(str, Enum):
    """Single authoritative lifecycle state of one calculation run."""

    NEW = "new"
    SUBMITTING = "submitting"
    QUEUED = "queued"
    RUNNING = "running"
    DOWNLOADING = "downloading"
    POSTPROCESSING = "postprocessing"
    FINISHED = "finished"
    FAILED = "failed"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"

    @property
    def is_terminal(self):
        """Whether this run is finished, failed or cancelled (not UNKNOWN)."""
        return self in (RunState.FINISHED, RunState.FAILED, RunState.CANCELLED)


