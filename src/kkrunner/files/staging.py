"""Atomic publication of locally staged files, independent of their source."""

import os
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from kkrunner.support.decorators import portable


def _local_temporary_path(destination):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    return destination.with_name(f".{destination.name}.{uuid4().hex}.tmp")


@contextmanager
def staged_local_file(destination, *, verify=None, overwrite=True):
    """One local staging and publication path for all stable file transfers.

    Internal temporary names are transport-private; the caller reserves the
    declared destination. Verification runs before the final atomic operation.
    """
    destination = Path(destination)
    temporary = _local_temporary_path(destination)
    try:
        yield temporary
        if verify is not None:
            verify(temporary)
        if overwrite:
            os.replace(temporary, destination)
        else:
            # Atomically publish without replacing even an external writer's file.
            os.link(temporary, destination)
    except BaseException:
        # Cleanup failure must not mask a transfer or verification failure.
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    else:
        temporary.unlink(missing_ok=True)


@portable
def publish_directory(staged, destination):
    """Publish a complete staged tree, retaining the old tree until publication."""
    import os
    import shutil
    from uuid import uuid4

    staged, destination = os.fspath(staged), os.fspath(destination)
    if os.path.islink(staged) or not os.path.isdir(staged):
        raise ValueError(f"Invalid staged directory: {staged}")
    if os.path.islink(destination) or (os.path.lexists(destination) and not os.path.isdir(destination)):
        raise ValueError(f"Directory destination is not a regular directory: {destination}")
    if not os.path.exists(destination):
        os.replace(staged, destination)
        return
    backup = os.path.join(os.path.dirname(destination), ".kkrunner-backup-" + uuid4().hex)
    os.replace(destination, backup)
    try:
        os.replace(staged, destination)
    except BaseException:
        os.replace(backup, destination)
        raise
    shutil.rmtree(backup)
