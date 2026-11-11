"""Portable direct-SSH target source; no target kkrunner dependency.

Generated from the same MPI resolver and output preparation functions used
by local execution, followed by the standalone subprocess worker.
"""
from inspect import getsource

from kkrunner.backends.support import resolve_mpi_options
from kkrunner.files.availability import prepare_target_outputs

# One implementation of each target rule; the remaining worker code owns only
# SSH-direct child process streams and cancellation.
WORKER_SOURCE = (
    getsource(resolve_mpi_options) + "\n" + getsource(prepare_target_outputs) + "\n" + r'''
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
    launcher = resolve_mpi_options(mpi)
    if not launcher:
        if mpi.get("required"):
            raise FileNotFoundError("Required remote MPI launcher is missing")
        return serial, request.get("stdin")
    return list(launcher) + parallel, (request.get("stdin") if request.get("mpi_uses_stdin", True) else None)

def execute(request):
    os.chdir(request["directory"])
    prepare_target_outputs(request)
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
        detached = bool(request.get("detached", False))
        child = subprocess.Popen(
            argv, cwd=request["directory"], stdin=stdin,
            stdout=(
                stream_files.get(request.get("output_file"), subprocess.DEVNULL)
                if detached else subprocess.PIPE
            ),
            stderr=(
                stream_files.get(request.get("error_file"), subprocess.DEVNULL)
                if detached else subprocess.PIPE
            ),
            start_new_session=True,
        )
        # Keep the control channel alive while the client remains attached:
        # detached execution must still support explicit cancel and wait.
        # Independent child streams make a broken SSH connection harmless.
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
                if not detached and child.poll() is None:
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
        if detached:
            # The child writes directly to its declared files (or DEVNULL).
            # No relay thread may depend on the SSH channel's pipes.
            returncode = child.wait()
        else:
            out = threading.Thread(target=pump, args=(child.stdout, sys.stdout.buffer, "out"))
            err = threading.Thread(target=pump, args=(child.stderr, sys.stderr.buffer, "err"))
            out.start()
            err.start()
            try:
                returncode = child.wait()
            finally:
                out.join()
                err.join()
        code = returncode
        return code if code >= 0 else 128 - code

try:
    sys.exit(execute(json.loads(sys.argv[1])))
except Exception as error:
    sys.stderr.write("SSH direct worker: " + str(error) + "\n")
    sys.exit(127)
'''
)



# The SSH static-method runner uses the same worker functions without the
# command-line bootstrap. No installed package imports occur on the target.
_BOOTSTRAP_MARKER = "\ntry:\n    sys.exit(execute(json.loads(sys.argv[1])))"
assert WORKER_SOURCE.count(_BOOTSTRAP_MARKER) == 1
DIRECT_LIBRARY_SOURCE = WORKER_SOURCE.split(_BOOTSTRAP_MARKER, 1)[0]
