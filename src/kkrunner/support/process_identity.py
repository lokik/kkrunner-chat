"""Persistable process identity using only psutil's public API."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass

import psutil

# One microsecond covers epoch-float cancellation, well below a Linux clock tick.
_TIME_TOLERANCE = 1e-6
_READ_ATTEMPTS = 3


def _same_time(first, second):
    return math.isclose(first, second, rel_tol=0.0, abs_tol=_TIME_TOLERANCE)


@dataclass(frozen=True)
class ProcessIdentity:
    """PID and creation time, relative to boot on Linux and to the epoch elsewhere.

    Reads are observations, not an atomic guarantee that a process remains alive.
    Failed or unstable reads never establish identity. Non-Linux creation times
    retain the platform's wall-clock and timestamp-resolution limitations.
    """

    pid: int
    start_time: float
    time_basis: str

    def __post_init__(self):
        if (
            type(self.pid) is not int
            or self.pid <= 0
            or type(self.start_time) not in (int, float)
            or not math.isfinite(self.start_time)
            or self.start_time < 0
            or self.time_basis not in ("linux-boot", "epoch")
        ):
            raise ValueError("Invalid process identity")

    def to_dict(self):
        return asdict(self)

    def to_token(self):
        """PID:start-time identity interpreted on the execution host."""
        return f"{self.pid}:{self.start_time:.9f}"

    @classmethod
    def from_token(cls, value):
        try:
            pid, started = value.split(":")
            return cls(int(pid), float(started), "linux-boot" if psutil.LINUX else "epoch")
        except (AttributeError, TypeError, ValueError, OverflowError) as exc:
            raise ValueError("Invalid process identity token") from exc

    @classmethod
    def from_dict(cls, value):
        if not isinstance(value, dict) or set(value) != {"pid", "start_time", "time_basis"}:
            raise ValueError("Invalid process identity")
        try:
            return cls(**value)
        except (OverflowError, TypeError) as exc:
            raise ValueError("Invalid process identity") from exc

    @classmethod
    def read(cls, pid):
        """Read a live process identity, or return None when it cannot be verified."""
        if type(pid) is not int or pid <= 0:
            return None
        linux = psutil.LINUX
        for _ in range(_READ_ATTEMPTS):
            try:
                # Older psutil also caches Linux boot time; refresh it before Process.
                before = psutil.boot_time() if linux else 0.0
                # create_time() is cached: each attempt needs a fresh Process.
                process = psutil.Process(pid)
                created = process.create_time()
                if process.status() in (psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD) or not process.is_running():
                    return None
                after = psutil.boot_time() if linux else 0.0
                # A clock adjustment between reads would corrupt the subtraction.
                if not _same_time(before, after):
                    continue
                return cls(pid, created - after, "linux-boot" if linux else "epoch")
            except (psutil.Error, OSError, ValueError, OverflowError, RuntimeError):
                return None
        return None

    def is_running(self):
        current = self.read(self.pid)
        return (
            current is not None
            and current.time_basis == self.time_basis
            and _same_time(current.start_time, self.start_time)
        )
