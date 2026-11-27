"""Persistent Python command loop sent over an SSH channel.

The remote host needs only Python's standard library, not kkrunner. Function
definitions live under generated Python names, not in a remote function cache.
"""

WORKER_SOURCE = r'''
import asyncio
import inspect
import json
import sys
from contextlib import redirect_stdout

_output = sys.stdout
_globals = {"__name__": "__kkrunner_session__"}
_output.write("KKRUNNER_SESSION_1\n")
_output.flush()

while True:
    _line = sys.stdin.buffer.readline()
    if not _line:
        break
    _installed = False
    try:
        _request = json.loads(_line)
        with redirect_stdout(sys.stderr):
            for _name, _source in _request["definitions"]:
                if not _name.startswith("_kkrunner_"):
                    raise ValueError("Invalid portable function identity")
                exec(_source, _globals, _globals)
            _installed = True
            _value = _globals[_request["function"]](
                *_request["args"], **_request["kwargs"]
            )
            if inspect.isawaitable(_value):
                _value = asyncio.run(_value)
            _reply = {"version": 1, "loaded": True, "result": _value}
            _wire = json.dumps(_reply, allow_nan=False, separators=(",", ":"))
    except Exception as _error:
        _reply = {"version": 1, "loaded": _installed, "error": {
            "type": type(_error).__name__,
            "message": str(_error),
            "submission_started": getattr(_error, "submission_started", None),
            "errno": getattr(_error, "errno", None),
        }}
        _wire = json.dumps(_reply, allow_nan=False, separators=(",", ":"))
    _output.write(_wire + "\n")
    _output.flush()
'''
