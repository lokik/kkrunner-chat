"""Slurm-native status and submission recovery."""

from __future__ import annotations

import hashlib
import shlex
import subprocess
from datetime import datetime

from kkrunner.backends.contracts import JobFailurePhase, JobStatus
from kkrunner.execution import RunState


_SLURM_STATE_MAP = {
    "PENDING": RunState.QUEUED,
    "CONFIGURING": RunState.QUEUED,
    "SUSPENDED": RunState.QUEUED,
    "RUNNING": RunState.RUNNING,
    "COMPLETING": RunState.RUNNING,
    "RESIZING": RunState.RUNNING,
    "COMPLETED": RunState.FINISHED,
    "CANCELLED": RunState.CANCELLED,
    "CANCELED": RunState.CANCELLED,
    "BOOT_FAIL": RunState.FAILED,
    "DEADLINE": RunState.FAILED,
    "FAILED": RunState.FAILED,
    "NODE_FAIL": RunState.FAILED,
    "OUT_OF_MEMORY": RunState.FAILED,
    "PREEMPTED": RunState.FAILED,
    "REVOKED": RunState.FAILED,
    "TIMEOUT": RunState.FAILED,
}


def _scheduler_output(argv):
    return subprocess.run(
        tuple(argv),
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ).stdout


def _slurm_state(value):
    value = (value or "").strip().upper().split("+", 1)[0]
    return _SLURM_STATE_MAP.get(value, RunState.UNKNOWN)


def _slurm_exit_code(value):
    if not value:
        return None
    code = value.split(":", 1)[0]
    try:
        return int(code)
    except ValueError:
        return None


def _slurm_details(native_id):
    output = _scheduler_output(("scontrol", "--oneliner", "show", "job", str(native_id)))
    fields = {}
    for token in shlex.split(output):
        name, separator, value = token.partition("=")
        if separator:
            fields[name] = value
    return fields


def submission_marker(run_id):
    """Short, scheduler-safe lookup marker for an arbitrary valid Run ID."""
    return "kkrunner:" + hashlib.blake2s(run_id.encode("utf8"), digest_size=16).hexdigest()


def _slurm_current_matches(run_id):
    marker = submission_marker(run_id)
    output = _scheduler_output(("squeue", "--noheader", "--me", "--format=%i|%k|%T|%Z"))
    matches = []
    for line in output.splitlines():
        fields = line.split("|", 3)
        if len(fields) != 4:
            continue
        native_id, comment, state, directory = (field.strip() for field in fields)
        if comment == marker:
            matches.append((native_id, state, directory))
    return tuple(matches)


def _slurm_accounting_stores_comments():
    output = _scheduler_output(("scontrol", "show", "config"))
    for line in output.splitlines():
        name, separator, value = line.partition("=")
        if separator and name.strip() == "AccountingStoreFlags":
            flags = {flag.strip().lower() for flag in value.split(",")}
            return "job_comment" in flags
    return False


def _slurm_time(value):
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone()
    return parsed.strftime("%Y-%m-%dT%H:%M:%S")


def _slurm_accounting_matches(run_id, submitted_at):
    marker = submission_marker(run_id)
    output = _scheduler_output(
        (
            "sacct",
            "-X",
            "-n",
            "-P",
            f"--starttime={_slurm_time(submitted_at)}",
            "--format=JobIDRaw,Comment,State,WorkDir,StdOut,StdErr,ExitCode",
        )
    )
    matches = []
    for line in output.splitlines():
        fields = line.rstrip("|").split("|")
        if len(fields) != 7:
            continue
        native_id, comment, state, directory, stdout, stderr, exit_code = fields
        if comment.strip() == marker:
            matches.append(
                (
                    native_id.strip(),
                    state.strip(),
                    directory.strip(),
                    stdout.strip() or None,
                    stderr.strip() or None,
                    _slurm_exit_code(exit_code.strip()),
                )
            )
    return tuple(matches)


def _job_status(state, return_code):
    if not state.is_terminal:
        return_code = None
    elif state == RunState.FINISHED and return_code is None:
        return_code = 0
    elif state == RunState.FINISHED and return_code != 0:
        state = RunState.FAILED
    return JobStatus(
        state=state, return_code=return_code,
        failure_phase=JobFailurePhase.EXECUTION if state == RunState.FAILED else None,
    )


def _controller_status(native_id):
    try:
        details = _slurm_details(native_id)
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    if not details:
        return None
    return _job_status(_slurm_state(details.get("JobState")), _slurm_exit_code(details.get("ExitCode")))


def _accounting_statuses(native_ids):
    if not native_ids:
        return {}
    try:
        output = _scheduler_output((
            "sacct", "-X", "-n", "-P", "-j", ",".join(native_ids),
            "--format=JobIDRaw,State,ExitCode",
        ))
    except (OSError, subprocess.SubprocessError):
        return {}
    requested = set(native_ids)
    result = {}
    for line in output.splitlines():
        fields = line.rstrip("|").split("|")
        if len(fields) == 3 and fields[0].strip() in requested:
            native_id, state, code = (field.strip() for field in fields)
            result.setdefault(native_id, _job_status(_slurm_state(state), _slurm_exit_code(code)))
    return result


def status(native_id: str) -> JobStatus:
    """Read scheduler facts for one local Slurm job without initializing PSI/J."""
    native_id = str(native_id)
    try:
        facts = _controller_status(native_id)
        if facts is not None:
            return facts
        return _accounting_statuses((native_id,)).get(native_id, JobStatus(RunState.UNKNOWN))
    except (OSError, subprocess.SubprocessError) as exc:
        return JobStatus(RunState.UNKNOWN, message=f"Cannot query execution status: {exc}")


def statuses(native_ids):
    """Query requested jobs together; preserve controller fallback if accounting lags."""
    ids = tuple(dict.fromkeys(map(str, native_ids)))
    if len(ids) <= 1:
        return {native_id: status(native_id) for native_id in ids}
    try:
        output = _scheduler_output((
            "squeue", "--noheader", "--jobs=" + ",".join(ids),
            "--states=all", "--format=%i|%T",
        ))
    except (OSError, subprocess.SubprocessError):
        # If the queue query itself failed, retain the authoritative per-job
        # controller query rather than treating stale accounting as live facts.
        return {native_id: status(native_id) for native_id in ids}
    requested = set(ids)
    result = {}
    for line in output.splitlines():
        fields = line.split("|")
        if len(fields) != 2:
            continue
        native_id, raw_state = (field.strip() for field in fields)
        state = _slurm_state(raw_state)
        if native_id in requested and state != RunState.UNKNOWN and not state.is_terminal:
            result[native_id] = _job_status(state, None)

    missing = tuple(native_id for native_id in ids if native_id not in result)
    accounting = _accounting_statuses(missing)
    for native_id in missing:
        facts = accounting.get(native_id)
        if facts is None or not facts.state.is_terminal:
            # Recently completed jobs may still be in the controller but not
            # in sacct (including clusters without accounting configured).
            try:
                facts = _controller_status(native_id) or facts
            except (OSError, subprocess.SubprocessError) as exc:
                facts = JobStatus(RunState.UNKNOWN, message=f"Cannot query execution status: {exc}")
        result[native_id] = facts if facts is not None else JobStatus(RunState.UNKNOWN)
    return result


def recover(run_id, directory, *, submitted_at=None):
    """Recover an uncertain local Slurm submission from scheduler facts."""
    matches = _slurm_current_matches(run_id)
    if len(matches) > 1:
        raise RuntimeError(f"Multiple live Slurm jobs carry KKRunner run id {run_id!r}")
    if matches:
        native_id, state, workdir = matches[0]
        try:
            details = _slurm_details(native_id)
        except (FileNotFoundError, subprocess.CalledProcessError):
            # The job may have finished between squeue and scontrol.
            # Recover its terminal facts from accounting instead.
            details = None
        if details:
            return {
                "native_id": str(native_id),
                "state": _slurm_state(state),
                "directory": details.get("WorkDir") or workdir or directory,
                "stdout": details.get("StdOut"),
                "stderr": details.get("StdErr"),
                "return_code": None,
            }

    if submitted_at is None or not _slurm_accounting_stores_comments():
        raise NotImplementedError(
            "Slurm cannot safely determine whether this submission already "
            "finished because job comments are unavailable in accounting"
        )
    accounting = _slurm_accounting_matches(run_id, submitted_at)
    if len(accounting) > 1:
        raise RuntimeError(f"Multiple Slurm accounting records carry KKRunner run id {run_id!r}")
    if not accounting:
        # An empty sacct result is not authoritative negative evidence:
        # Slurm accounting can lag behind the scheduler. Do not release
        # the client's SUBMITTING reservation and risk a duplicate job.
        raise NotImplementedError(
            "Slurm has no matching job in squeue or accounting yet; submission acceptance is still uncertain"
        )
    native_id, state, workdir, stdout, stderr, return_code = accounting[0]
    recovered_state = _slurm_state(state)
    if recovered_state == RunState.FINISHED and return_code not in (None, 0):
        recovered_state = RunState.FAILED
    return {
        "native_id": str(native_id),
        "state": recovered_state,
        "directory": workdir or directory,
        "stdout": stdout,
        "stderr": stderr,
        "return_code": return_code,
    }
