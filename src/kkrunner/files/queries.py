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


def remote_file_cleanup(paths, root=None, *, expected=None, kinds=None):
    """Inspect or remove exact declared paths, including whole directories.

    Portable stdlib-only target operation. The client first checks that each
    requested path is listed in the persisted FileManifest, then passes its
    declared kind. A declared file must be a file; a declared directory must
    be a directory. Recursive deletion requires the snapshot of every child.
    Unexpected changes cause that directory to be skipped.
    Errors on one target do not interrupt cleanup of other selected targets.
    """
    import os
    import shutil
    import stat

    if root is not None:
        root = os.path.normpath(os.fspath(root))
        if not os.path.isabs(root):
            raise ValueError("Cleanup root must be absolute")
        real_root = os.path.realpath(root)

    def snapshot(path):
        """Use lstat, so a symlink is observed rather than followed."""
        info = os.stat(path, follow_symlinks=False)
        kind = ("directory" if stat.S_ISDIR(info.st_mode) else
                "file" if stat.S_ISREG(info.st_mode) else
                "symlink" if stat.S_ISLNK(info.st_mode) else "other")
        return {
            "path": path, "kind": kind, "mtime_ns": info.st_mtime_ns,
            "size": info.st_size, "device": info.st_dev, "inode": info.st_ino,
        }

    def tree(path):
        """Sorted complete tree snapshot; scandir does not follow links."""
        members = []
        def walk(directory):
            with os.scandir(directory) as entries:
                children = sorted(entries, key=lambda item: item.name)
            for entry in children:
                detail = snapshot(entry.path)
                members.append(detail)
                if detail["kind"] == "directory":
                    walk(entry.path)
        walk(path)
        return members

    results = []
    for given in paths:
        result = {"path": given}
        try:
            original = os.fspath(given)
            if not os.path.isabs(original):
                raise ValueError("Remote cleanup requires absolute paths")
            if ".." in original.split(os.sep):
                raise ValueError("Parent traversal is not allowed")
            path = os.path.normpath(original)
            if root is not None:
                relative = os.path.relpath(path, root)
                if relative in (".", "..") or relative.startswith(".." + os.sep):
                    raise ValueError("Path outside the allowed root")
                if os.path.commonpath((real_root, os.path.realpath(path))) != real_root:
                    raise ValueError("Resolved path outside the allowed root")
            else:
                relative = path.lstrip(os.sep)

            cursor = os.sep if root is None else root
            if root is not None and os.path.islink(root):
                raise ValueError("Cleanup root is a symbolic link")
            for part in relative.split(os.sep):
                cursor = os.path.join(cursor, part)
                if os.path.islink(cursor):
                    raise ValueError("Path or parent is a symbolic link")

            try:
                state = snapshot(path)
            except FileNotFoundError:
                result["status"] = "missing"
                results.append(result)
                continue
            declared_kind = "file" if kinds is None else kinds.get(given)
            if declared_kind not in ("file", "directory"):
                raise ValueError("Missing or invalid declared filesystem kind")
            if state["kind"] != declared_kind:
                raise ValueError(
                    f"Declared {declared_kind} is a {state['kind']}; refusing cleanup"
                )
            members = tree(path) if declared_kind == "directory" else []
            result.update(state)
            result["contents"] = members
            if expected is None:
                result["status"] = "ok"
            else:
                confirmed = expected.get(given)
                # Existing consumers pass a four-number list for regular files.
                if isinstance(confirmed, list):
                    confirmed = {
                        "kind": "file", "mtime_ns": confirmed[0],
                        "size": confirmed[1], "device": confirmed[2],
                        "inode": confirmed[3], "contents": [],
                    }
                if confirmed is None or any(
                    confirmed.get(key) != result.get(key)
                    for key in ("kind", "mtime_ns", "size", "device", "inode", "contents")
                ):
                    result["status"] = "changed"
                    result["message"] = "File or directory contents changed since confirmation"
                else:
                    # Recheck everything just before deletion, including
                    # additions and removal of descendants since preview.
                    latest = snapshot(path)
                    fresh = tree(path) if latest["kind"] == "directory" else []
                    if latest != state or fresh != members:
                        result["status"] = "changed"
                        result["message"] = "File or directory changed during cleanup"
                    else:
                        if state["kind"] == "directory":
                            shutil.rmtree(path)
                        else:
                            os.unlink(path)
                        result["status"] = "deleted"
        except (OSError, ValueError) as exc:
            result["status"] = "skipped"
            result["message"] = str(exc)
        results.append(result)
    return results
