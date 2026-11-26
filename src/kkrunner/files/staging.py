"""Atomic publication of locally staged files, independent of their source."""

import os
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4


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
