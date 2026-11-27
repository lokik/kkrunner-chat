"""Content-aware identity for files used by calculation provenance."""

from __future__ import annotations

import hashlib
import stat as stat_module
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from kkrunner.files.fingerprints import FileFingerprint, file_fingerprint
from kkrunner.files.queries import directory_digest

_DIGEST_CHUNK_SIZE = 1024 * 1024
_STABLE_CAPTURE_ATTEMPTS = 2


class FileState(str, Enum):
    """Relationship between a captured file version and its path on disk now."""

    CURRENT = "current"
    OVERWRITTEN = "overwritten"
    MISSING = "missing"
    UNAVAILABLE = "unavailable"


class FileVersionCaptureError(OSError):
    """A stable content identity could not be captured for an existing file."""


def _matches_kind(fingerprint: FileFingerprint, kind: str) -> bool:
    return (stat_module.S_ISDIR(fingerprint.mode) if kind == "directory"
            else stat_module.S_ISREG(fingerprint.mode))


def file_digest(path: str | Path, *, missing_ok: bool = False) -> str | None:
    """SHA-256 of a file; optionally return None if the file is missing."""
    digest = hashlib.sha256()
    try:
        with Path(path).open("rb") as stream:
            while chunk := stream.read(_DIGEST_CHUNK_SIZE):
                digest.update(chunk)
    except FileNotFoundError:
        if missing_ok:
            return None
        raise
    return digest.hexdigest()


def _fingerprint(value) -> FileFingerprint | None:
    if value is None or isinstance(value, FileFingerprint):
        return value
    if isinstance(value, dict):
        return FileFingerprint(**value)
    return FileFingerprint(*value)


@dataclass(frozen=True, eq=False)
class FileVersion:
    """A path together with cheap metadata and authoritative content identity.

    ``fingerprint`` is a cheap filesystem snapshot used only as a fast path.
    For regular files captured with the default settings, ``digest`` is the
    SHA-256 identity of the contents. A later metadata change therefore causes
    content verification rather than by itself making the historical version
    stale.
    """

    path: Path
    fingerprint: FileFingerprint | None
    digest: str | None = None
    kind: str = "file"

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", Path(self.path).expanduser().absolute())
        object.__setattr__(self, "fingerprint", _fingerprint(self.fingerprint))
        if self.kind not in ("file", "directory"):
            raise ValueError("FileVersion kind must be file or directory")

    @classmethod
    def capture(
        cls,
        path: str | Path,
        *,
        digest: bool = True,
        require_digest: bool = False,
        kind: str = "file",
    ) -> "FileVersion":
        """Capture *path*, optionally requiring a stable content digest.

        A digest is never associated with metadata observed before or after a
        concurrent rewrite. Strict callers such as persistent-history capture
        retry once and then fail instead of degrading to metadata-only identity.
        """
        if require_digest and not digest:
            raise ValueError("require_digest requires digest=True")
        resolved = Path(path).expanduser().absolute()
        try:
            fingerprint = file_fingerprint(resolved)
        except OSError:
            if require_digest:
                raise
            return cls(path=resolved, fingerprint=None, kind=kind)

        if not _matches_kind(fingerprint, kind):
            if require_digest:
                raise FileVersionCaptureError(f"Expected {kind} at {resolved}")
            return cls(path=resolved, fingerprint=fingerprint, kind=kind)
        if not digest:
            return cls(path=resolved, fingerprint=fingerprint, kind=kind)

        digest_file = directory_digest if kind == "directory" else file_digest
        last_error = None
        for _ in range(_STABLE_CAPTURE_ATTEMPTS if require_digest else 1):
            try:
                content_digest = digest_file(resolved)
                after = file_fingerprint(resolved)
            except OSError as exc:
                last_error = exc
                if require_digest:
                    continue
                return cls(path=resolved, fingerprint=fingerprint, kind=kind)

            if after == fingerprint and content_digest is not None:
                return cls(
                    path=resolved,
                    fingerprint=fingerprint,
                    digest=content_digest,
                    kind=kind,
                )
            fingerprint = after
            if not _matches_kind(fingerprint, kind):
                break

        if require_digest:
            message = f"Cannot capture stable contents of {resolved}"
            if last_error is not None:
                raise FileVersionCaptureError(message) from last_error
            raise FileVersionCaptureError(message)
        return cls(path=resolved, fingerprint=fingerprint, kind=kind)

    @property
    def available(self) -> bool:
        """Whether this snapshot has usable content identity."""
        return self.digest is not None and (
            self.fingerprint is None or _matches_kind(self.fingerprint, self.kind)
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FileVersion):
            return NotImplemented
        if self.path != other.path or self.kind != other.kind:
            return False
        if self.digest is not None or other.digest is not None:
            return self.digest is not None and other.digest is not None and self.digest == other.digest
        return self.fingerprint == other.fingerprint

    def __hash__(self) -> int:
        if self.digest is not None:
            return hash((self.path, self.kind, self.digest))
        return hash((self.path, self.kind, self.fingerprint))

    def current(self) -> "FileVersion":
        """Return this version if current, otherwise capture the version on disk."""
        if self.is_current():
            return self
        return type(self).capture(self.path, kind=self.kind)

    def is_current(self) -> bool:
        return self.state() is FileState.CURRENT

    def state(self) -> FileState:
        """Return the current relationship to the captured version."""
        try:
            before = file_fingerprint(self.path)
        except FileNotFoundError:
            return FileState.MISSING
        except OSError:
            return FileState.UNAVAILABLE

        if not _matches_kind(before, self.kind):
            return FileState.OVERWRITTEN
        if self.kind == "file" and self.fingerprint is not None and before == self.fingerprint:
            return FileState.CURRENT
        if self.digest is None:
            return FileState.OVERWRITTEN

        try:
            digest_file = directory_digest if self.kind == "directory" else file_digest
            digest = digest_file(self.path)
            after = file_fingerprint(self.path)
        except FileNotFoundError:
            return FileState.MISSING
        except OSError:
            return FileState.UNAVAILABLE
        if before != after:
            return FileState.OVERWRITTEN
        return FileState.CURRENT if digest == self.digest else FileState.OVERWRITTEN


__all__ = [
    "FileState",
    "FileVersion",
    "FileVersionCaptureError",
    "file_digest",
]
