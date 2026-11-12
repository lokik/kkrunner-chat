"""Persistable process identity using psutil and the Linux kernel boot ID."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from pathlib import Path
from uuid import UUID

import psutil

# One microsecond covers epoch-float cancellation, well below a Linux clock tick.
_TIME_TOLERANCE = 1e-6
_READ_ATTEMPTS = 3


def _same_time(first, second):
    return math.isclose(first, second, rel_tol=0.0, abs_tol=_TIME_TOLERANCE)


def _read_boot_id():
    try:
        return str(UUID(Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()))
    except (OSError, ValueError):
        return None


@dataclass(frozen=True)
class ProcessIdentity:
    """PID and creation time, scoped to a kernel boot on Linux.

    Time is relative to boot on Linux and to the epoch elsewhere.

    Reads are observations, not an atomic guarantee that a process remains alive.
    Failed or unstable reads never establish identity. Non-Linux creation times
    retain the platform's wall-clock and timestamp-resolution limitations.
    """

    pid: int
    start_time: float
    time_basis: str
    boot_id: str | None = None

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
        if self.boot_id is not None:
            try:
                valid_boot = self.time_basis == "linux-boot" and str(UUID(self.boot_id)) == self.boot_id
            except (ValueError, TypeError, AttributeError):
                valid_boot = False
            if not valid_boot:
                raise ValueError("Invalid process boot identity")

    def to_dict(self):
        return asdict(self)

    def to_token(self):
        """PID:start-time identity, additionally scoped to a Linux boot."""
        token = f"{self.pid}:{self.start_time:.9f}"
        return token if self.boot_id is None else f"{token}:{self.boot_id}"

    @classmethod
    def from_token(cls, value):
        try:
            parts = value.split(":")
            if len(parts) not in (2, 3):
                raise ValueError("Invalid process identity token")
            pid, started = parts[:2]
            return cls(
                int(pid), float(started), "linux-boot" if psutil.LINUX else "epoch",
                parts[2] if len(parts) == 3 else None,
            )
        except (AttributeError, TypeError, ValueError, OverflowError) as exc:
            raise ValueError("Invalid process identity token") from exc

    @classmethod
    def from_dict(cls, value):
        required = {"pid", "start_time", "time_basis"}
        if not isinstance(value, dict):
            raise ValueError("Invalid process identity")
        fields = value.keys()
        if not required <= fields or fields - required - {"boot_id"}:
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
        boot_id = _read_boot_id() if linux else None
        if linux and boot_id is None:
            return None  # A Linux PID/start-time pair cannot establish boot identity.
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
                return cls(pid, created - after, "linux-boot" if linux else "epoch", boot_id)
            except (psutil.Error, OSError, ValueError, OverflowError, RuntimeError):
                return None
        return None

    def is_running(self):
        current = self.read(self.pid)
        return (
            current is not None
            and current.time_basis == self.time_basis
            and current.boot_id == self.boot_id
            and _same_time(current.start_time, self.start_time)
        )
