"""In-process file-transfer double for tests of remote execution policy."""

import asyncio
import os
import shutil
from pathlib import Path

from kkrunner.targets import LocalTarget
from kkrunner.files.queries import local_file_signature
from kkrunner.files.staging import staged_directory, staged_local_file
from kkrunner.files.transfer import relative_file_pairs, transfer_path


class LoopbackFileTransfer(LocalTarget):
    """Simulate a separate target filesystem without an SSH connection."""

    is_local = False
    supports_file_transfer = True

    async def put_file(self, source, destination, *, verify=None, overwrite=True):
        source = os.fspath(source)
        signature = local_file_signature(source)
        if signature is None:
            raise FileNotFoundError(source)
        destination = Path(destination)
        try:
            if os.path.samefile(source, destination):
                return
        except FileNotFoundError:
            pass  # A new destination will be published by atomic replacement.
        with staged_local_file(destination, verify=verify, overwrite=overwrite) as temporary:
            await asyncio.to_thread(shutil.copyfile, source, temporary)
            if local_file_signature(source) != signature:
                raise RuntimeError(f"Local file changed while copying: {source}")
            stat = temporary.stat()
            os.utime(temporary, ns=(stat.st_atime_ns, signature[1]))

    # On a local filesystem both directions are the same stable copy.
    get_file = put_file

    async def put_directory(self, source, destination):
        """Copy a whole tree before replacing the destination."""
        source, destination = Path(source), Path(destination)
        if source.is_symlink() or not source.is_dir():
            raise ValueError(f"Directory source is invalid: {source}")
        if destination.is_symlink():
            raise ValueError(f"Cannot replace symbolic link with directory: {destination}")
        source_path, destination_path = source.resolve(), destination.resolve()
        if source_path == destination_path:
            return
        if source_path in destination_path.parents or destination_path in source_path.parents:
            raise ValueError("Directory source and destination overlap")
        with staged_directory(destination) as received:
            await asyncio.to_thread(shutil.copytree, source, received, dirs_exist_ok=True)

    get_directory = put_directory

    async def _transfer_files(self, files, source_directory, destination_directory, transfer):
        source_root = Path(source_directory).expanduser().resolve()
        destination_root = Path(destination_directory).expanduser().resolve()
        pairs = relative_file_pairs(files, source_root, destination_root)
        result = []
        for source, destination in pairs:
            source = transfer_path(source_root, source, source=True)
            destination = transfer_path(destination_root, destination, source=False)
            await transfer(source, destination)
            result.append((os.fspath(source), os.fspath(destination)))
        return tuple(result)

    async def put_files(self, files, source_directory, destination_directory):
        return await self._transfer_files(files, source_directory, destination_directory, self.put_file)

    async def get_files(self, files, source_directory, destination_directory):
        return await self._transfer_files(files, source_directory, destination_directory, self.get_file)

