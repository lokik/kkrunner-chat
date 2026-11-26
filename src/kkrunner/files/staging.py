"""Atomic publication of locally staged files, independent of their source."""

from contextlib import contextmanager

from kkrunner.support.decorators import depends_on, portable


@portable
def _local_temporary_path(destination):
    from pathlib import Path
    from uuid import uuid4

    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    return destination.with_name(f".{destination.name}.{uuid4().hex}.tmp")


@portable
@depends_on(_local_temporary_path)
def _staged_local_file(destination, *, verify=None, overwrite=True):
    """One local staging and publication path for all stable file transfers.

    Internal temporary names are transport-private; the caller reserves the
    declared destination. Verification runs before the final atomic operation.
    """
    import os
    from pathlib import Path

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


staged_local_file = contextmanager(_staged_local_file)


@portable
def publish_directory(staged, destination):
    """Publish a complete staged tree, retaining the old tree until publication."""
    import os
    import tempfile
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
    # The old tree is now disposable. Use TemporaryDirectory's permission-
    # aware cleanup (also used for staging) rather than plain rmtree, which
    # fails on read-only subdirectories. Keep the backup outside this context
    # until publication has succeeded, so rollback failure cannot delete it.
    with tempfile.TemporaryDirectory(prefix=".kkrunner-cleanup-",
                                     dir=os.path.dirname(destination) or ".") as cleanup:
        os.replace(backup, os.path.join(cleanup, "previous"))


@portable
@depends_on(publish_directory)
def _staged_directory(destination):
    """Create and publish one private tree on the destination filesystem."""
    import tempfile
    from pathlib import Path

    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".kkrunner-tree-", dir=destination.parent) as stage:
        tree = Path(stage).resolve() / "tree"
        tree.mkdir()
        yield tree
        publish_directory(tree, destination)


staged_directory = contextmanager(_staged_directory)
