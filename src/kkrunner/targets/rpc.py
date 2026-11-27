"""Build remote Python calls and decode their JSON replies.

Both local and remote calls share the same JSON value contract.
"""

from __future__ import annotations

import json

from kkrunner.backends.contracts import SubmissionNotStarted


class RemoteOperationError(RuntimeError):
    """The target ran the operation and reported an application failure."""

def operation_payload(args, kwargs):
    """Encode arguments using the same JSON contract for both targets."""
    return json.dumps({"args": args, "kwargs": kwargs}, allow_nan=False)


def operation_result(payload):
    """Decode a target reply independently of SSH process/connection failures."""
    try:
        reply = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise RemoteOperationError("Invalid backend operation reply") from exc
    if not isinstance(reply, dict) or reply.get("version") != 1:
        raise RemoteOperationError("Invalid backend operation reply")
    if "error" not in reply:
        if "result" not in reply:
            raise RemoteOperationError("Backend operation reply has no result")
        return reply["result"]
    error = reply["error"]
    if (
        "result" in reply or not isinstance(error, dict)
        or not isinstance(error.get("type"), str) or not isinstance(error.get("message"), str)
    ):
        raise RemoteOperationError("Invalid backend operation error")
    message = error["message"]
    if error.get("submission_started") is False:
        raise SubmissionNotStarted(message)
    # Reconstruct only explicitly supported errors; never import a remote type.
    errors = {kind.__name__: kind for kind in (
        ValueError, TypeError, FileNotFoundError, PermissionError, IsADirectoryError,
        NotADirectoryError, FileExistsError, NotImplementedError, OSError,
    )}
    kind = errors.get(error.get("type"))
    if kind is not None:
        if issubclass(kind, OSError):
            raise kind(error.get("errno"), message)
        raise kind(message)
    raise RemoteOperationError(f"{error.get('type', 'RemoteError')}: {message}")
