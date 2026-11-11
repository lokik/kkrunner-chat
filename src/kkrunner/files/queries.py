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
