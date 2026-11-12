"""Shared backend operations and stdlib-level process/MPI support."""
from __future__ import annotations

import asyncio
import os
import signal

from kkrunner.execution import MpiSpec


def prepare_mpi(request, *, resolve_only=False):
    """Materialize MPI on the execution host; standalone for source delivery."""
    import errno
    import os
    import shutil

    prepared = dict(request)
    result = {"request": prepared, "launcher": None, "warning": None, "use_stdin": True}
    mpi = request.get("mpi") or {}
    if request.get("mpi_argv") is None:
        return result
    launcher = tuple(mpi.get("launcher") or ())
    requested = mpi.get("required") or mpi.get("autodetect") or launcher
    if requested and not launcher:
        affinity = getattr(os, "sched_getaffinity", None)
        serial = (mpi.get("autodetect") and not mpi.get("required")
                  and affinity is not None and len(affinity(0)) == 1)
        if not serial:
            if os.environ.get("SLURM_NODELIST") and shutil.which("srun"):
                launcher = ("srun",)
            else:
                for candidate in ("mpirun", "mpirun.opmpirun", "mpirun.mpich", "mpiexec"):
                    if shutil.which(candidate):
                        launcher = (candidate,)
                        break
    if not launcher:
        if not resolve_only:
            if mpi.get("required"):
                raise FileNotFoundError(errno.ENOENT, "MPI was requested but no MPI launcher was found")
            if mpi.get("autodetect") and mpi.get("warning", True):
                result["warning"] = "MPI autodetection did not find an MPI launcher; the calculation will run without MPI."
        return result
    if mpi.get("process_count") is not None:
        option = "-n" if os.path.basename(os.fspath(launcher[0])) == "srun" else "-np"
        launcher = (*launcher, option, str(mpi["process_count"]))
    result["launcher"] = list(launcher)
    prepared["argv"] = [*launcher, *request["mpi_argv"]]
    if not request.get("mpi_uses_stdin", True):
        prepared["stdin"] = None
        result["use_stdin"] = False
    return result


def resolve_mpi_options(options):
    """Query the same target resolver without requiring an executable command."""
    reply = prepare_mpi({"argv": [], "mpi_argv": [], "mpi": options}, resolve_only=True)
    launcher = reply["launcher"]
    return None if launcher is None else tuple(launcher)


def resolve_mpi_command(spec: MpiSpec):
    """Adapt the validated native MpiSpec to the portable target resolver."""
    if not isinstance(spec, MpiSpec):
        raise TypeError("resolve_mpi_command() requires an MpiSpec")
    return resolve_mpi_options({
        "required": spec.required,
        "autodetect": spec.autodetect,
        "launcher": spec.launcher,
        "process_count": spec.process_count,
    })


async def pump_stream(source, sink, *, eof=None):
    """Forward one subprocess byte stream to a synchronous chunk consumer."""
    try:
        while data := await source.read(65536):
            sink(data)
    finally:
        if eof is not None:
            eof()


async def stop_local_process(
    process,
    *,
    process_group=False,
    timeout=0.1,
    waiting=None,
):
    """Send SIGTERM, then SIGKILL on timeout, without cancelling process.wait().

    For a process group, signal even if its leader has already exited:
    descendants may still be alive and need the full SIGTERM grace period.
    ``waiting`` reuses an existing wait task.
    """
    if process is None or (process.returncode is not None and not process_group):
        return

    if waiting is None:
        waiting = asyncio.create_task(process.wait())

    try:
        if process_group:
            os.killpg(process.pid, signal.SIGTERM)
        else:
            process.terminate()
    except ProcessLookupError:
        await waiting
        return

    deadline = asyncio.get_running_loop().time() + timeout
    try:
        await asyncio.wait_for(asyncio.shield(waiting), timeout)
    except asyncio.TimeoutError:
        if process.returncode is not None and not process_group:
            return
    else:
        if not process_group:
            return
        # The group can outlive its leader. Preserve the SIGTERM grace
        # period if any descendant still belongs to the group.
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            return
        await asyncio.sleep(max(0, deadline - asyncio.get_running_loop().time()))

    try:
        if process_group:
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
    except ProcessLookupError:
        pass
    await waiting
