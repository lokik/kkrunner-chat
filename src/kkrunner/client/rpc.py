"""Decode operation replies separately from failures of the SSH transport."""

from kkrunner.backends.contracts import SubmissionNotStarted


class RemoteOperationError(RuntimeError):
    """The target ran the operation and reported an application failure."""


def operation_result(reply):
    if not isinstance(reply, dict) or reply.get("version") != 1:
        raise RemoteOperationError("Invalid backend operation reply")
    if "error" not in reply:
        if "result" not in reply:
            raise RemoteOperationError("Backend operation reply has no result")
        return reply["result"]
    error = reply["error"]
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
