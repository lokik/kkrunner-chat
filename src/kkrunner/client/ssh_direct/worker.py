"""Self-contained Python target worker sent over SSH; no kkrunner installation.

Its pipes are application stdout/stderr, not a control protocol. Cancellation is
the single 'cancel' command on stdin. Keeping the target-side implementation
isolated makes it possible to replace this strategy without touching the
supervised process backend.
"""

WORKER_SOURCE = r'''
import glob
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
from contextlib import ExitStack

def choose_command(request):
    serial = request["argv"]
    parallel = request.get("mpi_argv")
    mpi = (request.get("launch") or {}).get("mpi") or {}
    if not parallel or not (mpi.get("required") or mpi.get("autodetect") or mpi.get("launcher")):
        return serial, request.get("stdin")
    launcher = mpi.get("launcher") or []
    if not launcher:
        if os.environ.get("SLURM_NODELIST") and shutil.which("srun"):
            launcher = ["srun"]
        else:
            for candidate in ("mpirun", "mpirun.opmpirun", "mpirun.mpich", "mpiexec"):
                if shutil.which(candidate):
                    launcher = [candidate]
                    break
    if not launcher:
        if mpi.get("required"):
            raise FileNotFoundError("Required remote MPI launcher is missing")
        return serial, request.get("stdin")
    launcher = list(launcher)
    if mpi.get("process_count") is not None:
        flag = "-n" if os.path.basename(launcher[0]) == "srun" else "-np"
        launcher.extend((flag, str(mpi["process_count"])))
    return launcher + parallel, (request.get("stdin") if request.get("mpi_uses_stdin", True) else None)

def clean_outputs(request):
    """Apply the same exact-output vs wildcard protection as local execution.

    Protected cache/input patterns must be expanded on the execution host;
    the client cannot safely guess which target files already exist.
    """
    sync = request.get("client_sync") or {}
    streams = {p for p in (request.get("output_file"), request.get("error_file")) if p}
    protected_declarations = [
        *request.get("input_files", ()),
        request.get("stdin"),
        request.get("input_file"),
        *(item["path"] for item in sync.get("cache_files", ())),
        *sync.get("cache_patterns", ()),
        *(item["path"] for item in sync.get("cache_before", ())),
    ]
    protected = set()
    for declaration in protected_declarations:
        if declaration is None:
            continue
        matches = (glob.glob(declaration, recursive=True) if glob.has_magic(declaration)
                   else [declaration])
        for candidate in matches:
            protected.add(os.path.realpath(candidate))

    def is_protected(path):
        return os.path.realpath(path) in protected

    patterns = [*sync.get("output_files", ()), *sync.get("output_patterns", ())]
    for path in streams:
        if is_protected(path):
            raise ValueError("Output stream conflicts with input/cache: " + path)
    for declaration in dict.fromkeys(patterns):
        wildcard = glob.has_magic(declaration)
        matches = (glob.glob(declaration, recursive=True) if wildcard else [declaration])
        for path in matches:
            if path in streams:
                continue
            if is_protected(path):
                if wildcard:
                    continue  # Preserve caches/inputs matched by broad output patterns.
                raise ValueError("Explicit output conflicts with input/cache: " + path)
            if os.path.islink(path) or os.path.isfile(path):
                os.unlink(path)
            elif os.path.exists(path):
                if wildcard:
                    continue  # Match local cleanup: only delete regular files.
                raise IsADirectoryError(path)

def execute(request):
    os.chdir(request["directory"])
    clean_outputs(request)
    argv, stdin_path = choose_command(request)
    with ExitStack() as resources:
        stdin = (resources.enter_context(open(stdin_path, "rb"))
                 if stdin_path else subprocess.DEVNULL)
        stream_files = {}
        for kind, target in (("out", request.get("output_file")),
                             ("err", request.get("error_file"))):
            if target is not None:
                os.makedirs(os.path.dirname(os.path.abspath(target)), exist_ok=True)
                if target not in stream_files:
                    stream_files[target] = resources.enter_context(open(target, "wb", buffering=0))
        child = subprocess.Popen(
            argv, cwd=request["directory"], stdin=stdin,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            start_new_session=True,
        )
        stopping = threading.Event()
        def stop():
            if stopping.is_set():
                return
            stopping.set()
            try:
                os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError:
                return
            def force_stop():
                # A child may ignore SIGTERM. Kill its entire group after a
                # short grace period, including surviving descendants.
                import time
                time.sleep(1.0)
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            threading.Thread(target=force_stop, daemon=True).start()
        def control():
            try:
                for line in sys.stdin.buffer:
                    if line.strip() == b"cancel":
                        stop()
                        return
            finally:
                # An unexpected SSH disconnect closes stdin. Without this,
                # the remote calculation could outlive the direct Job.
                if child.poll() is None:
                    stop()
        threading.Thread(target=control, daemon=True).start()
        stream_lock = threading.Lock()
        def pump(source, destination, kind):
            target = request.get("output_file" if kind == "out" else "error_file")
            while True:
                data = source.read(65536)
                if not data:
                    return
                if target is not None:
                    with stream_lock:
                        stream_files[target].write(data)
                try:
                    destination.write(data)
                    destination.flush()
                except BrokenPipeError:
                    stop()
                    return
        out = threading.Thread(target=pump, args=(child.stdout, sys.stdout.buffer, "out"))
        err = threading.Thread(target=pump, args=(child.stderr, sys.stderr.buffer, "err"))
        out.start()
        err.start()
        try:
            code = child.wait()
        finally:
            out.join()
            err.join()
        return code if code >= 0 else 128 - code

try:
    sys.exit(execute(json.loads(sys.argv[1])))
except Exception as error:
    sys.stderr.write("SSH direct worker: " + str(error) + "\n")
    sys.exit(127)
'''
