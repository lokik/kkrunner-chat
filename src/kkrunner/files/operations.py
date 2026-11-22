"""Short-lived local filesystem observations with explicit refresh semantics.

FileOperations does not own transfers, scientific attribution, run history or
SSH. Content identity is delegated to the existing FileVersion implementation.

Each cached observation is independent. Refreshing one entry does not make a
batch-wide snapshot: refresh dependent entries or invalidate() after changes.
"""

from __future__ import annotations

import glob as glob_module
import os
import stat as stat_module
from pathlib import Path

from .fingerprints import FileFingerprint
from .versions import FileVersion


class FileOperations:
    """Short-lived cache of filesystem observations, not thread-safe.

    Make one instance per operation/import batch and discard it afterwards.
    Neither TaskResult nor RunRecord should retain this cache.
    """

    def __init__(self, *, cache: bool = True):
        self.cache = cache
        self.invalidate()

    def invalidate(self) -> None:
        """Forget all observations, including glob and directory snapshots."""
        self._stats: dict[Path, os.stat_result] = {}
        self._directories: dict[Path, tuple[str, ...]] = {}
        self._globs: dict[tuple[str, bool], tuple[Path, ...]] = {}
        self._versions: dict[tuple[Path, bool, bool, str], FileVersion] = {}

    @staticmethod
    def _path(path: str | os.PathLike) -> Path:
        # Preserve the symlinked path's namespace, not its resolved target.
        return Path(path).expanduser().absolute()

    def _drop_versions(self, path: Path) -> None:
        for key in tuple(self._versions):
            if key[0] == path:
                del self._versions[key]

    def _drop_inconsistent_stat(self, version: FileVersion) -> None:
        """A separately captured version must not leave outdated stat data."""
        old = self._stats.get(version.path)
        if old is not None and (
            version.fingerprint is None
            or FileFingerprint.from_stat(old) != version.fingerprint
        ):
            self._stats.pop(version.path, None)

    def stat(self, path: str | os.PathLike, *, refresh: bool = False) -> os.stat_result:
        """Return os.stat information; refresh writes through to the cache."""
        path = self._path(path)
        if self.cache and not refresh and path in self._stats:
            return self._stats[path]
        try:
            result = os.stat(path)
        except OSError:
            if self.cache and refresh:
                self._stats.pop(path, None)
                self._drop_versions(path)
            raise
        if self.cache:
            self._stats[path] = result
            if refresh:
                # Metadata alone cannot validate a content digest.  This
                # matters especially for directory versions: descendants
                # may change without changing the parent's stat data.
                self._drop_versions(path)
        return result

    def exists(self, path: str | os.PathLike, *, refresh: bool = False) -> bool:
        """Return False for missing path components, propagate other errors."""
        try:
            self.stat(path, refresh=refresh)
        except (FileNotFoundError, NotADirectoryError):
            return False
        return True

    def is_file(self, path: str | os.PathLike, *, refresh: bool = False) -> bool:
        """Whether a path names a regular file, following symlinks."""
        try:
            info = self.stat(path, refresh=refresh)
        except (FileNotFoundError, NotADirectoryError):
            return False
        return stat_module.S_ISREG(info.st_mode)

    def fingerprint(self, path: str | os.PathLike, *, refresh: bool = False) -> FileFingerprint:
        """Cheap file identity using the same stat observation."""
        return FileFingerprint.from_stat(self.stat(path, refresh=refresh))

    def listdir(self, directory: str | os.PathLike, *, refresh: bool = False) -> tuple[str, ...]:
        """Sorted entry names including dotfiles."""
        directory = self._path(directory)
        if self.cache and not refresh and directory in self._directories:
            return self._directories[directory]
        try:
            names = tuple(sorted(os.listdir(directory)))
        except OSError:
            if self.cache and refresh:
                self._directories.pop(directory, None)
            raise
        if self.cache:
            self._directories[directory] = names
            if refresh:
                # Listing a directory does not validate a previous tree
                # digest or the directory's cached stat observation.
                self._stats.pop(directory, None)
                self._drop_versions(directory)
        return names

    def iterdir(self, directory: str | os.PathLike, *, refresh: bool = False) -> tuple[Path, ...]:
        """Path counterparts of listdir without another directory read."""
        directory = self._path(directory)
        return tuple(directory / name for name in self.listdir(directory, refresh=refresh))

    def glob(self, pattern: str | os.PathLike, *, recursive: bool = True,
             refresh: bool = False) -> tuple[Path, ...]:
        """Sorted Python glob results, not root-confined remote discovery."""
        key = os.fspath(self._path(pattern)), recursive
        if self.cache and not refresh and key in self._globs:
            return self._globs[key]
        paths = tuple(sorted(Path(name) for name in glob_module.glob(key[0], recursive=recursive)))
        if self.cache:
            self._globs[key] = paths
        return paths

    def capture_version(self, path: str | os.PathLike, *, digest: bool = True,
                        require_digest: bool = False, kind: str = "file",
                        refresh: bool = False) -> FileVersion:
        """Capture content identity using FileVersion, never a separate digest.

        Missing paths or inconclusive digest attempts are not cached.
        """
        if require_digest and not digest:
            raise ValueError("require_digest requires digest=True")
        key = self._path(path), digest, require_digest, kind
        if self.cache and not refresh and key in self._versions:
            return self._versions[key]
        try:
            version = FileVersion.capture(key[0], digest=digest, require_digest=require_digest, kind=kind)
        except OSError:
            if self.cache:
                # A failed fresh capture disproves all previous cached versions
                # of this path, even when refresh=False was requested.
                self._stats.pop(key[0], None)
                self._drop_versions(key[0])
            raise
        if self.cache:
            # FileVersion.capture deliberately performs its own fresh pre/post
            # reads around hashing.  An observed change must invalidate ALL
            # cached variants of this path, not only the requested digest mode.
            self._drop_inconsistent_stat(version)
            incompatible = any(
                existing_key[0] == key[0] and (
                    cached.fingerprint != version.fingerprint
                    or (
                        cached.digest is not None and version.digest is not None
                        and cached.digest != version.digest
                    )
                )
                for existing_key, cached in self._versions.items()
            )
            if refresh or incompatible:
                self._drop_versions(key[0])
            if version.fingerprint is not None and (not digest or version.available):
                self._versions[key] = version
        return version

__all__ = ["FileOperations"]
