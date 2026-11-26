"""Exact batch transfer declarations and streaming archive primitives."""

from pathlib import PurePath, PurePosixPath
import os

from kkrunner.files.staging import _staged_local_file, _staged_directory
from kkrunner.files.queries import directory_entries
from kkrunner.support.decorators import depends_on, portable


def relative_file_pairs(files, source_directory, destination_directory, *,
                        source_path=PurePath, destination_path=PurePath):
    """Normalize exact paths without globbing or resolving their identities.

    A single name denotes the same relative path on both sides. A pair
    explicitly specifies a rename. Absolute paths must be within their root.
    """
    def relative(value, directory, path_type):
        path = path_type(os.fsdecode(value))
        if path.is_absolute():
            path = path.relative_to(path_type(directory))
        if not path.parts or path.anchor or ".." in path.parts:
            raise ValueError(f"File is outside transfer directory: {value}")
        return path.as_posix()

    if isinstance(files, (str, bytes, os.PathLike)):
        files = (files,)
    pairs = {}
    for value in files:
        if isinstance(value, (str, bytes, os.PathLike)):
            source = relative(value, source_directory, source_path)
            destination = source
        else:
            source, destination = value
            source = relative(source, source_directory, source_path)
            destination = relative(destination, destination_directory, destination_path)
        previous = pairs.setdefault(destination, source)
        if previous != source:
            raise ValueError(f"Different sources map to {destination}")
    return tuple((source, destination) for destination, source in pairs.items())


@portable
def archive_reader(stream, compression):
    """Use a standard-library reader that checks the compression trailer at EOF."""
    from contextlib import nullcontext

    if not compression:
        return nullcontext(stream)
    if compression == "gz":
        from gzip import GzipFile
        return GzipFile(fileobj=stream, mode="rb")
    if compression == "bz2":
        from bz2 import BZ2File
        return BZ2File(stream, mode="rb")
    if compression == "xz":
        from lzma import LZMAFile
        return LZMAFile(stream, mode="rb")
    if compression == "zst":
        from compression.zstd import ZstdFile
        return ZstdFile(stream, mode="rb")
    raise ValueError(f"Unknown archive compression: {compression}")


@portable
@depends_on(archive_reader)
def archive_compressions():
    """Probe the actual tarfile streaming codecs, not the Python version."""
    import io
    import tarfile

    supported = []
    for name in ("gz", "bz2", "xz", "zstd"):
        mode = "zst" if name == "zstd" else name
        buffer = io.BytesIO()
        try:
            with tarfile.open(fileobj=buffer, mode="w|" + mode) as archive:
                archive.addfile(tarfile.TarInfo("probe"))
            buffer.seek(0)
            with archive_reader(buffer, mode) as stream:
                with tarfile.open(fileobj=stream, mode="r|") as archive:
                    if archive.next().name != "probe":
                        continue
                    while archive.fileobj.read(tarfile.RECORDSIZE):
                        pass
                supported.append(name)
        except (tarfile.CompressionError, ImportError):
            continue
    return supported


@portable
def transfer_path(root, name, *, source):
    """Validate an exact archive member, without expanding path patterns."""
    from pathlib import Path, PurePosixPath

    relative = PurePosixPath(name)
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise ValueError(f"Invalid transfer path: {name}")
    root = Path(root)
    path = root.joinpath(*relative.parts)
    if source:
        path.resolve().relative_to(root)
    elif path.parent.resolve() != path.parent:
        raise ValueError(f"Transfer destination crosses a symbolic link: {path}")
    return path


@portable
@depends_on(transfer_path, directory_entries)
def send_archive(directory, compression="", tree=False):
    """Stream exact pairs or one complete tree with tarfile's native codec."""
    import json
    import os
    import sys
    import tarfile
    from pathlib import Path

    def tree_entries(root):
        if not root.is_dir():
            raise ValueError(f"Invalid source directory: {root}")
        for name, kind, _ in directory_entries(root):
            if kind is None:
                raise ValueError(f"Invalid directory member: {root / name}")
            yield Path(name).as_posix()

    root = Path(directory)
    if tree and root.is_symlink():
        raise ValueError(f"Invalid source directory: {root}")
    root = root.resolve()
    files = tree_entries(root) if tree else json.load(sys.stdin.buffer)
    with tarfile.open(fileobj=sys.stdout.buffer, mode="w|" + compression,
                      format=tarfile.PAX_FORMAT, dereference=True) as archive:
        if tree:
            archive.addfile(archive.gettarinfo(root, arcname="."))
        for value in files:
            source, destination = (value, value) if isinstance(value, str) else value
            path = transfer_path(root, source, source=True)
            if tree and path.is_dir() and not path.is_symlink():
                archive.addfile(archive.gettarinfo(path, arcname=destination))
                continue
            if not path.is_file():
                raise ValueError(f"Not a regular transfer file: {path}")
            with path.open("rb") as stream:
                before = os.fstat(stream.fileno())
                info = archive.gettarinfo(fileobj=stream, arcname=destination)
                if not info.isreg():
                    raise ValueError(f"Not a regular transfer file: {path}")
                info.pax_headers["kkrunner.mtime_ns"] = str(before.st_mtime_ns)
                archive.addfile(info, stream)
                after = path.stat()
                if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
                    after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns
                ):
                    raise RuntimeError(f"File changed while transferring: {path}")
        # A tar EOF by itself is insufficient: closing an archive on an
        # exception can still write a valid trailer.
        marker = tarfile.TarInfo(".kkrunner-complete")
        marker.pax_headers["kkrunner.complete"] = "1"
        archive.addfile(marker)


@portable
@depends_on(transfer_path, _staged_local_file, _staged_directory, archive_reader)
def receive_archive(directory, compression="", tree=False):
    """Stage exact files beside their targets, or a complete tree beside its root."""
    import os
    import shutil
    import sys
    import tarfile
    from contextlib import ExitStack, contextmanager
    from pathlib import Path

    seen = set()
    directories = []
    with ExitStack() as staged:
        root = (staged.enter_context(contextmanager(_staged_directory)(directory))
                if tree else Path(directory).resolve())
        with archive_reader(sys.stdin.buffer, compression) as stream:
            with tarfile.open(fileobj=stream, mode="r|") as archive:
                for member in archive:
                    if member.pax_headers.get("kkrunner.complete") == "1":
                        if not member.isreg() or member.size or member.name != ".kkrunner-complete":
                            raise ValueError("Invalid transfer completion marker")
                        # Read from tarfile's stream, including its read-ahead
                        # buffer. The sender writes two zero blocks and pads to
                        # RECORDSIZE. Nothing else may follow the marker.
                        size = 2 * tarfile.BLOCKSIZE
                        size += -(archive.fileobj.tell() + size) % tarfile.RECORDSIZE
                        if archive.fileobj.read(size) != bytes(size):
                            raise ValueError("Invalid or incomplete TAR trailer")
                        # Reading EOF via the decoder checks the compressed
                        # stream's footer, including its checksum if present.
                        if archive.fileobj.read(1):
                            raise ValueError("Unexpected data after transfer completion")
                        break
                    if tree and member.isdir() and member.name == ".":
                        destination = root
                    else:
                        destination = transfer_path(root, member.name, source=False)
                    if destination in seen:
                        raise ValueError(f"Duplicate transfer destination: {destination}")
                    seen.add(destination)
                    if tree and member.isdir():
                        destination.mkdir(parents=True, exist_ok=True)
                        directories.append((destination, member.mode & 0o777))
                        continue
                    if not member.isreg():
                        raise ValueError(f"Not a regular archive member: {member.name}")
                    if tree:
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        temporary = destination
                    else:
                        temporary = staged.enter_context(contextmanager(_staged_local_file)(destination))
                    with archive.extractfile(member) as incoming, temporary.open("wb") as output:
                        shutil.copyfileobj(incoming, output, 1024 * 1024)
                    timestamp = int(member.pax_headers["kkrunner.mtime_ns"])
                    os.utime(temporary, ns=(temporary.stat().st_atime_ns, timestamp))
                    os.chmod(temporary, member.mode & 0o777)
                else:
                    raise RuntimeError("Incomplete file transfer")
        # Child directories first; restrictive permissions must not impede
        # extraction or traversal while restoring descendant permissions.
        for destination, mode in sorted(directories, key=lambda item: len(item[0].parts), reverse=True):
            os.chmod(destination, mode)
