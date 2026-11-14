"""Portable filesystem queries, independent of transports and job execution."""

import errno
import os
from pathlib import Path


def _sources(sources):
    if isinstance(sources, (str, bytes, os.PathLike)):
        return (sources,)
    return tuple(sources)


def query_files(sources, *, root=None, digest=False):
    """Return JSON-safe metadata, including root-constrained wildcard matches.

    This function is deliberately standalone: SSH can execute its source on a
    target with Python stdlib but without kkrunner. No module-level helpers,
    annotations, decorators or nonliteral default arguments are required.
    """
    import glob
    import hashlib
    import os
    import stat
    from pathlib import Path

    if isinstance(sources, (str, bytes, os.PathLike)):
        sources = (sources,)
    else:
        sources = tuple(sources)

    if root is None:
        paths = [os.fsdecode(path) for path in sources]
        real_root = None
    else:
        absolute_root = Path(os.path.abspath(os.path.expanduser(os.fspath(root))))
        real_root = absolute_root.resolve()
        paths = []
        for source in sources:
            given = Path(os.fspath(source)).expanduser()
            path = given if given.is_absolute() else absolute_root / given
            pattern = os.path.abspath(os.fspath(path))
            try:
                relative = os.path.relpath(pattern, absolute_root)
            except ValueError as exc:
                raise ValueError(f"File pattern {source!r} is outside source root {root}") from exc
            if relative == os.pardir or relative.startswith(os.pardir + os.sep):
                raise ValueError(f"File pattern {source!r} is outside source root {root}")
            matches = sorted(glob.glob(pattern, recursive=True)) if glob.has_magic(pattern) else [pattern]
            paths.extend(os.path.abspath(match) for match in matches)
        paths = list(dict.fromkeys(paths))

    results = []
    for path in paths:
        try:
            info = os.stat(path)
            if real_root is not None:
                resolved = Path(path).resolve()
                try:
                    resolved.relative_to(real_root)
                except ValueError as exc:
                    raise ValueError(f"Expanded file {resolved} is outside source root {root}") from exc
            kind = "file" if stat.S_ISREG(info.st_mode) else "directory" if stat.S_ISDIR(info.st_mode) else "other"
            item = {"path": path, "status": "ok", "size": info.st_size, "mtime_ns": info.st_mtime_ns, "type": kind}
            if digest and kind == "file":
                sha = hashlib.sha256()
                with open(path, "rb") as stream:
                    for block in iter(lambda: stream.read(1024 * 1024), b""):
                        sha.update(block)
                item["digest"] = sha.hexdigest()
        except FileNotFoundError:
            item = {"path": path, "status": "missing"}
        except OSError as exc:
            item = {"path": path, "status": "error", "error": {"errno": exc.errno, "message": str(exc)}}
        results.append(item)
    return results


def checked_files(results):
    """Raise genuine filesystem errors, leaving missing entries explicit."""
    for item in results:
        if item["status"] == "error":
            error = item["error"]
            raise OSError(error.get("errno") or errno.EIO, error["message"], item["path"])
    return results


def expand_local_files(sources, root):
    """Select regular files using the same metadata/discovery path as SSH."""
    return tuple(
        dict.fromkeys(
            os.fspath(Path(item["path"]).resolve())
            for item in checked_files(query_files(sources, root=root))
            if item["status"] == "ok" and item["type"] == "file"
        )
    )


def local_file_signature(path):
    """Return ``(size, mtime_ns)`` or None for a missing local path."""
    item = checked_files(query_files([path]))[0]
    return None if item["status"] == "missing" else (item["size"], item["mtime_ns"])


def finalize_upload(temporary, destination, mtime_ns):
    """Atomically publish a completed upload; standalone on remote Python."""
    import os

    info = os.stat(temporary)
    os.utime(temporary, ns=(info.st_atime_ns, int(mtime_ns)))
    os.replace(temporary, destination)


def remote_file_cleanup(paths, root=None, *, expected=None):
    """Inspect/delete exact regular paths, including auxiliary directories.

    This is a self-contained, stdlib-only SSH operation. The client must first
    verify that every requested path is declared in the run's FileManifest.
    An optional root further confines paths when requested, but a run's
    execution directory is not a suitable universal boundary: declared files
    (for example TAUDIR/TAUPATH outputs) may live elsewhere.

    Only exact paths are processed; directories, symlinks, and unverified files
    cannot be deleted. expected supplies the previously reviewed identity
    (mtime_ns, size, device, inode) for each requested file.
    """
    import os
    import stat

    if root is not None:
        root = os.path.normpath(os.fspath(root))
        if not os.path.isabs(root):
            raise ValueError("Remote cleanup root must be absolute")
        real_root = os.path.realpath(root)

    results = []
    deleting = expected is not None
    for given in paths:
        result = {"path": given}
        try:
            original = os.fspath(given)
            if not os.path.isabs(original):
                raise ValueError("Remote cleanup requires absolute file paths")
            if ".." in original.split(os.sep):
                raise ValueError("Parent traversal is not allowed")
            path = os.path.normpath(original)

            if root is not None:
                relative = os.path.relpath(path, root)
                if relative in (".", "..") or relative.startswith(".." + os.sep):
                    raise ValueError("File is outside the allowed root")
                if os.path.commonpath((real_root, os.path.realpath(path))) != real_root:
                    raise ValueError("Resolved file escapes the allowed root")
            else:
                relative = path.lstrip(os.sep)

            # Reject symbolic links at every path component. This is
            # intentionally independent of the execution directory: a TAUDIR
            # can be outside it but still must be an explicitly declared path.
            cursor = os.sep if root is None else root
            if root is not None and os.path.islink(root):
                raise ValueError("Cleanup root is a symbolic link")
            for part in relative.split(os.sep):
                cursor = os.path.join(cursor, part)
                if os.path.islink(cursor):
                    raise ValueError("A file or parent is a symbolic link")

            try:
                info = os.stat(path, follow_symlinks=False)
            except FileNotFoundError:
                result["status"] = "missing"
            else:
                if not stat.S_ISREG(info.st_mode):
                    raise ValueError("Not a regular file")
                identity = [info.st_mtime_ns, info.st_size, info.st_dev, info.st_ino]
                result.update(
                    mtime_ns=info.st_mtime_ns, size=info.st_size,
                    device=info.st_dev, inode=info.st_ino,
                )
                if not deleting:
                    result["status"] = "ok"
                elif given not in expected or list(expected[given]) != identity:
                    result["status"] = "changed"
                    result["message"] = "File changed since preview"
                else:
                    # Recheck each file independently; one failed path never
                    # prevents cleanup of another approved path.
                    latest = os.stat(path, follow_symlinks=False)
                    if not stat.S_ISREG(latest.st_mode) or [
                        latest.st_mtime_ns, latest.st_size, latest.st_dev, latest.st_ino
                    ] != identity:
                        result["status"] = "changed"
                        result["message"] = "File changed during cleanup"
                    else:
                        os.unlink(path)
                        result["status"] = "deleted"
        except (OSError, ValueError) as exc:
            result["status"] = "skipped"
            result["message"] = str(exc)
        results.append(result)
    return results
