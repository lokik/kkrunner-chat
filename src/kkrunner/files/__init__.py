"""File declarations, transfer manifests and captured file identity.

Imports of this package are lightweight; concrete interfaces load on demand.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kkrunner.files.operations import FileOperations
    from kkrunner.files.fingerprints import FileFingerprint
    from kkrunner.files.manifest import ArtifactAvailabilityState, CacheEntry, FileEntry, FileManifest, FileMapping, FileSyncPolicy
    from kkrunner.files.task_file import TaskFile
    from kkrunner.files.versions import FileState, FileVersion, FileVersionCaptureError

__all__ = [
    "ArtifactAvailabilityState",
    "CacheEntry",
    "FileEntry",
    "FileFingerprint",
    "FileOperations",
    "FileManifest",
    "FileMapping",
    "FileState",
    "FileSyncPolicy",
    "FileVersion",
    "FileVersionCaptureError",
    "TaskFile",
]


def __getattr__(name):
    if name == "FileOperations":
        from . import operations
        value = getattr(operations, name)
    elif name == "FileFingerprint":
        from .fingerprints import FileFingerprint as value
    elif name in (
        "ArtifactAvailabilityState",
        "CacheEntry",
        "FileEntry",
        "FileManifest",
        "FileMapping",
        "FileSyncPolicy",
    ):
        from . import manifest
        value = getattr(manifest, name)
    elif name == "TaskFile":
        from .task_file import TaskFile as value
    elif name in (
        "FileState",
        "FileVersion",
        "FileVersionCaptureError",
    ):
        from . import versions
        value = getattr(versions, name)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value

def __dir__():
    return sorted(set(globals()) | set(__all__))
