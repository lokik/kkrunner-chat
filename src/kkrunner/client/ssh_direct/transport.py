"""SSH transport operations using only target Python stdlib (no agent)."""

from __future__ import annotations

import json

from kkrunner.client.transport import SshTransport
from kkrunner.files.queries import _sources, checked_files

_QUERY_SOURCE = r'''
import glob
import hashlib
import json
import os
import stat
import sys

request = json.loads(sys.argv[1])
root = request.get("root")
real_root = os.path.realpath(os.path.expanduser(root)) if root is not None else None
paths = []
for source in request["paths"]:
    if root is None:
        paths.append(source)
        continue
    source = os.path.expanduser(source)
    path = os.path.abspath(source if os.path.isabs(source) else os.path.join(root, source))
    if os.path.commonpath((os.path.abspath(os.path.expanduser(root)), path)) != os.path.abspath(os.path.expanduser(root)):
        raise ValueError("Pattern escapes mapped root: " + path)
    matches = glob.glob(path, recursive=True) if glob.has_magic(path) else [path]
    paths.extend(sorted(matches))
results = []
for path in dict.fromkeys(paths):
    try:
        info = os.stat(path)
        if real_root is not None and os.path.commonpath((real_root, os.path.realpath(path))) != real_root:
            raise ValueError("Expanded file escapes mapped root: " + path)
        kind = "file" if stat.S_ISREG(info.st_mode) else "directory" if stat.S_ISDIR(info.st_mode) else "other"
        item = {"path": path, "status": "ok", "size": info.st_size, "mtime_ns": info.st_mtime_ns, "type": kind}
        if request.get("digest") and kind == "file":
            digest = hashlib.sha256()
            with open(path, "rb") as stream:
                for part in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(part)
            item["digest"] = digest.hexdigest()
    except OSError as exc:
        if isinstance(exc, FileNotFoundError):
            item = {"path": path, "status": "missing"}
        else:
            item = {"path": path, "status": "error", "error": {"errno": exc.errno, "message": str(exc)}}
    results.append(item)
print(json.dumps(results, separators=(",", ":")))
'''

_FINALIZE_SOURCE = r'''
import os
import sys
temporary, destination, mtime_ns = sys.argv[1:]
info = os.stat(temporary)
os.utime(temporary, ns=(info.st_atime_ns, int(mtime_ns)))
os.replace(temporary, destination)
'''


class SshDirectTransport(SshTransport):
    """Regular SSH/SCP-like streams and tar transfer without kkrunner-agent."""

    def __init__(self, host, *, ssh="ssh", options=(), zstd=False):
        super().__init__(host, ssh=ssh, options=options, zstd=zstd, command=())

    async def _finalize_upload(self, temporary, destination, mtime_ns):
        await self.run(("python3", "-c", _FINALIZE_SOURCE,
                        temporary, destination, str(mtime_ns)))

    async def query_files(self, sources, *, root=None, digest=False):
        result = await self.run((
            "python3", "-c", _QUERY_SOURCE,
            json.dumps({
                "paths": [str(path) for path in _sources(sources)],
                "root": None if root is None else str(root),
                "digest": digest,
            }),
        ))
        return checked_files(json.loads(result.stdout.decode("utf-8")))
