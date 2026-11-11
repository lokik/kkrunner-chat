"""Installed supervisor target operations."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
from pathlib import Path
from uuid import uuid4

from kkrunner.remote.handles import decode_handle, encode_handle
from kkrunner.backends.support.messages import (
    AcknowledgeReply, ExecutionRequest, RecoveryReply, RecoveryRequest, StatusReply, SubmissionReply,
    request_to_spec, status_reply,
)
from kkrunner.files.availability import prepare_target_outputs
from kkrunner.runs.base import FailurePhase, RunState, SubmissionNotStarted
from kkrunner.support.process_identity import ProcessIdentity
from kkrunner.backends.supervised import mailbox
from kkrunner.backends.supervised.worker import _SPEC_ENV, _START_FD_ENV
from kkrunner.backends.supervised.streams import follow_socket, SupervisorClosedBeforeFollow

def _absolute(directory, value):
    if value is None:
        return None
    path = Path(value)
    if not path.is_absolute():
        path = Path(directory) / path
    return os.fspath(path)


def _clean_request_outputs(spec, directory, output_path=None, error_path=None):
    """Adapt supervised process declarations to the shared target-side rules."""
    def paths(values):
        return [_absolute(directory, value) for value in values if value is not None]

    prepare_target_outputs({
        "input_file": _absolute(directory, spec.input_file),
        "stdin": _absolute(directory, spec.stdin),
        "input_files": paths(spec.input_files),
        "output_file": output_path,
        "error_file": error_path,
        "client_sync": {
            "output_files": paths(spec.output_files),
            "cache_files": [{"path": path} for path in paths(spec.cache_files)],
        },
    })


def _process_outcome(receipt_id):
    data = mailbox.read_outcome(receipt_id)
    if data is None:
        return None
    try:
        state = RunState(data["state"])
        return_code = data.get("return_code")
        raw_phase = data.get("failure_phase")
        failure_phase = None if raw_phase is None else FailurePhase(raw_phase)
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("Invalid remote process outcome artifact") from exc
    return state, return_code, failure_phase, data.get("error")
