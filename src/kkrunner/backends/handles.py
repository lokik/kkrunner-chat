"""Portable backend job handles.

Opaque to clients: the contents are validated and interpreted only on the
execution host.  Job handles do not define client Run identity.
"""

import base64
import binascii
import json
import re
import os
from pathlib import Path

from kkrunner.support.process_identity import ProcessIdentity

def _encode_handle(data):
    payload = json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")

def _decode_handle(value):
    if not isinstance(value, str):
        raise ValueError("Invalid remote execution id")
    padding = "=" * (-len(value) % 4)
    try:
        payload = base64.b64decode(value + padding, altchars=b"-_", validate=True)
        data = json.loads(payload.decode("utf8"))
    except (binascii.Error, UnicodeError, ValueError, TypeError) as exc:
        raise ValueError("Invalid remote execution id") from exc
    if not isinstance(data, dict):
        raise ValueError("Invalid remote execution id")
    kind = data.get("kind")
    if kind == "process":
        try:
            ProcessIdentity.from_dict(data.get("identity"))
        except ValueError as exc:
            raise ValueError("Invalid remote execution id") from exc
        valid = (
            isinstance(data.get("receipt_id"), str)
            and re.fullmatch(r"[0-9a-f]{32}", data["receipt_id"]) is not None
            and isinstance(data.get("socket_path"), str)
            and bool(data["socket_path"])
        )
    elif kind == "psij":
        valid = (
            isinstance(data.get("backend"), str)
            and bool(data["backend"])
            and (data.get("url") is None or isinstance(data["url"], str))
            and isinstance(data.get("native_id"), str)
            and bool(data["native_id"])
        )
    else:
        valid = False
    if not valid:
        raise ValueError("Invalid remote execution id")
    return data

def remote_stream_paths(remote_id):
    """Return declared output streams carried by a backend handle, if known.

    Historical handles have no file metadata. This does not query or persist
    runner lifecycle state; paths are hints for user-facing observation only.
    """
    handle = _decode_handle(remote_id)
    directory = handle.get("directory")

    def absolute(name):
        if name is None:
            return None
        path = Path(name)
        if not path.is_absolute() and directory is not None:
            path = Path(directory) / path
        return os.fspath(path)

    return {"output_file": absolute(handle.get("output_file")),
            "error_file": absolute(handle.get("error_file"))}

