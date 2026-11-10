"""Host-local MPI resolution, also runnable on targets without kkrunner."""

from __future__ import annotations

# Expose host modules for local diagnostics; the portable resolver imports
# them inside its own function to remain independent when transported.
import os
import shutil

from kkrunner.runs.base import MpiSpec


def resolve_mpi_options(options):
    """Resolve JSON-compatible MPI intent on the host performing the execution.

    Standalone stdlib-only function: transported verbatim by SSH-direct.
    """
    import os
    import shutil

    if not (options.get("required") or options.get("autodetect") or options.get("launcher")):
        return None
    launcher = tuple(options.get("launcher") or ())
    if not launcher:
        if options.get("autodetect") and not options.get("required"):
            affinity = getattr(os, "sched_getaffinity", None)
            if affinity is not None and len(affinity(0)) == 1:
                return None
        if os.environ.get("SLURM_NODELIST") and shutil.which("srun"):
            launcher = ("srun",)
        else:
            for candidate in ("mpirun", "mpirun.opmpirun", "mpirun.mpich", "mpiexec"):
                if shutil.which(candidate):
                    launcher = (candidate,)
                    break
        if not launcher:
            return None

    process_count = options.get("process_count")
    if process_count is not None:
        count_option = "-n" if os.path.basename(os.fspath(launcher[0])) == "srun" else "-np"
        launcher = (*launcher, count_option, str(process_count))
    return tuple(launcher)


def find_default_mpi_launcher():
    """Expose the shared target resolution for host-local diagnostics."""
    return resolve_mpi_options({"required": True, "autodetect": True})


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
