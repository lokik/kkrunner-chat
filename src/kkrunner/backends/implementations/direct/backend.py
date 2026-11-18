"""One portable process implementation, called locally or delivered over SSH."""

from __future__ import annotations

from typing import TYPE_CHECKING

from kkrunner.backends.contracts import Backend, BackendCapability, DirectOperations
from kkrunner.backends.support.process import prepare_mpi
from kkrunner.files.availability import prepare_target_outputs
from kkrunner.support.decorators import depends_on, portable

from .process import observe_process

if TYPE_CHECKING:
    from typing import Awaitable, BinaryIO, Callable, Mapping, Optional, Union
    from kkrunner.backends.contracts import AsyncByteSink, ByteSink, ControlReader
    from kkrunner.backends.support.messages import ExecutionRequest, ExecutionReply


class DirectBackend(Backend, DirectOperations):
    """Session-owned execution without a supervisor or durable outcome receipt."""

    name = "direct"
    prepare_mpi = staticmethod(prepare_mpi)
    prepare_target_outputs = staticmethod(prepare_target_outputs)

    def __init__(self, **options):
        super().__init__(**options)
        self.supports = frozenset({BackendCapability.CANCEL, BackendCapability.STATUS, BackendCapability.REATTACH})

    @staticmethod
    @portable
    @depends_on(observe_process)
    def status(remote_id: str):
        """Observe termination of the whole job, without asserting success."""
        state, error = observe_process(remote_id)
        return {"state": state, "return_code": None,
                "failure_phase": None, "error": error}

    @staticmethod
    @portable
    @depends_on(observe_process)
    async def follow(
        remote_id, stdout, stderr, *,
        stdout_offset=0, stderr_offset=0, poll_interval=0.2,
        stdout_path=None, stderr_path=None, started=None,
    ):
        """Observe saved direct output without owning or restarting the job.

        The operation is source-portable to SSH hosts without kkrunner. A
        legacy opaque ID can be observed but cannot prove termination.
        Cancelling this observer never signals the calculation.
        """
        import asyncio
        import inspect
        import os
        if type(poll_interval) not in (int, float) or not 0.05 <= poll_interval <= 10:
            raise ValueError("Invalid follow interval")
        offsets = {"out": stdout_offset, "err": stderr_offset}
        paths = {"out": stdout_path, "err": stderr_path}
        sinks = {"out": stdout, "err": stderr}
        for kind, offset in offsets.items():
            if offset is not None and (type(offset) is not int or offset < 0):
                raise ValueError(f"Invalid {kind} output offset")
        if paths["out"] is not None and paths["out"] == paths["err"]:
            # Independent offsets cannot reconstruct an interleaved file.
            paths["err"] = None

        async def copy_available():
            copied = False
            for kind, path in paths.items():
                offset = offsets[kind]
                if path is None or offset is None:
                    continue
                try:
                    with open(path, "rb") as stream:
                        stream.seek(0, os.SEEK_END)
                        size = stream.tell()
                        if size < offset:
                            raise RuntimeError(f"Observed output was truncated: {path}")
                        stream.seek(offset)
                        data = stream.read(65536)
                except FileNotFoundError:
                    continue  # The calculation may not have written it yet.
                if data:
                    pending = sinks[kind].write(data)
                    if inspect.isawaitable(pending):
                        await pending
                    sinks[kind].flush()
                    offsets[kind] += len(data)
                    copied = True
            return copied

        while True:
            copied = await copy_available()
            if observe_process(remote_id)[0] == "finished":
                while await copy_available():
                    pass  # Deliver final bytes already on disk.
                return {"state": "finished", "return_code": None,
                        "failure_phase": None,
                        "error": "Direct process ended; exit code unavailable"}
            if not copied:
                await asyncio.sleep(poll_interval)

    @staticmethod
    @portable
    @depends_on(observe_process)
    def cancel(remote_id: str, *, timeout: float = 1.0):
        """Signal the original direct process group using a verified Linux identity.

        This operation is source-portable: it requires only the target's Python
        standard library, not a target-side KKRunner installation. A legacy
        random session ID cannot prove a PID and must not be used for killing.
        """
        import os
        import signal
        import time
        from uuid import UUID

        parts = remote_id.split(":") if isinstance(remote_id, str) else ()
        if len(parts) != 4 or parts[0] != "direct":
            raise NotImplementedError(
                "This direct run has no persistent PID/start-time identity; "
                "cancellation after restart is not safe for older runs."
            )
        try:
            pid = int(parts[1])
            started = int(parts[2])
            boot = str(UUID(parts[3]))
            if pid <= 0 or started < 0 or boot != parts[3]:
                raise ValueError("Invalid direct process reference")
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("Invalid direct process reference") from exc
        if type(timeout) not in (int, float) or not 0 <= timeout <= 30:
            raise ValueError("Cancellation timeout must be between 0 and 30 seconds")

        def reply(state, message=None):
            return {
                "state": state,
                "return_code": None,
                "failure_phase": None,
                "error": message,
            }

        current, error = observe_process(remote_id)
        if current == "finished":
            return reply("finished", "The original process is no longer running; exit status unavailable")
        if current != "running":
            return reply("unknown", error)

        try:
            os.killpg(pid, signal.SIGTERM)
        except ProcessLookupError:
            return reply("finished", "Process exited before cancellation; exit status unavailable")

        def wait_for_exit(duration):
            deadline = time.monotonic() + duration
            while True:
                current, error = observe_process(remote_id)
                remaining = deadline - time.monotonic()
                if current != "running" or remaining <= 0:
                    return current, error
                time.sleep(min(0.05, remaining))

        current, error = wait_for_exit(timeout)

        if current == "running":
            # Observation also verifies surviving descendants after leader exit.
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                current = "finished"
            else:
                # SIGKILL delivery is asynchronous too. Confirm termination
                # before persisting cancellation, with a bounded extra wait.
                current, error = wait_for_exit(min(1.0, max(0.1, timeout)))

        if current == "finished":
            return reply("cancelled")
        return reply(
            "running" if current == "running" else "unknown",
            "Cancellation signal sent; termination not yet verified"
            if current == "running" else error,
        )

    @staticmethod
    @portable
    async def stream(
        request: ExecutionRequest, *,
        stdout: Optional[Union[ByteSink, AsyncByteSink]] = None,
        stderr: Optional[Union[ByteSink, AsyncByteSink]] = None,
        control: Optional[ControlReader] = None,
        files: Optional[Mapping[str, BinaryIO]] = None,
        started: Optional[Callable[[ExecutionReply], Union[None, Awaitable[None]]]] = None,
        stdin: Optional[BinaryIO] = None,
    ) -> ExecutionReply:
        """Run a process with explicit local streams or SSH standard streams.

        The operation owns the child, its pipe readers and group termination.
        Borrowed sinks may implement synchronous or asynchronous write(). The
        control reader supplies lines: ``cancel`` terminates the process group;
        EOF does so only for attached execution. Calculation stdin is separate.
        Acceptance and completion carry a session identity and native status.
        On Linux, the identity includes PID/start ticks/boot ID for safe later
        cancellation; elsewhere it stays opaque and cannot be used to kill.
        """
        import asyncio
        import inspect
        import os
        import signal
        import sys
        from uuid import uuid4
        from contextlib import ExitStack

        detached = bool(request.get("detached", False))
        identity = None
        cancelled = False

        def outcome(state, code=None, *, phase=None, error=None):
            return {
                "state": state, "return_code": code, "failure_phase": phase,
                "error": error, "remote_id": identity,
            }

        async def execute():
            nonlocal identity, cancelled, control
            child = control_transport = stopping = None
            tasks = []
            with ExitStack() as resources:
                # A blocking SSH stdout write must not stall the control reader.
                # Borrow the interpreter descriptors only when no explicit local
                # sink was supplied; restore their flags when this operation ends.
                descriptors = {}
                if not detached:
                    for kind, sink, stream in (("out", stdout, sys.stdout), ("err", stderr, sys.stderr)):
                        if sink is None:
                            fd = stream.fileno()
                            resources.callback(os.set_blocking, fd, os.get_blocking(fd))
                            os.set_blocking(fd, False)
                            descriptors[kind] = fd

                async def write_pipe(fd, data):
                    loop = asyncio.get_running_loop()
                    remaining = memoryview(data)
                    while remaining:
                        try:
                            written = os.write(fd, remaining)
                        except BlockingIOError:
                            writable = loop.create_future()

                            def ready():
                                loop.remove_writer(fd)
                                if not writable.done():
                                    writable.set_result(None)

                            loop.add_writer(fd, ready)
                            try:
                                await writable
                            finally:
                                loop.remove_writer(fd)
                        else:
                            remaining = remaining[written:]

                stdin_path = request.get("stdin")
                input_stream = stdin if stdin is not None else (
                    resources.enter_context(open(stdin_path, "rb")) if stdin_path else asyncio.subprocess.DEVNULL
                )
                forwarded_input = None
                if hasattr(input_stream, "read"):
                    try:
                        input_stream.fileno()
                    except (AttributeError, OSError, ValueError):
                        forwarded_input, input_stream = input_stream, asyncio.subprocess.PIPE
                outputs = dict(files or {})
                opened = {}
                for kind, path in (("out", request.get("output_file")), ("err", request.get("error_file"))):
                    if kind not in outputs and path is not None:
                        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
                        if path not in opened:
                            opened[path] = resources.enter_context(open(path, "wb", buffering=0))
                        outputs[kind] = opened[path]

                async def terminate_group():
                    try:
                        os.killpg(child.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        return
                    # Descendants can outlive their leader. Finish the grace period
                    # even when child.wait() has already returned.
                    await asyncio.sleep(1)
                    try:
                        os.killpg(child.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

                def stop():
                    nonlocal stopping
                    if stopping is None:
                        stopping = asyncio.create_task(terminate_group())

                async def read_control():
                    nonlocal cancelled
                    try:
                        while line := await control.readline():
                            if line.strip() == b"cancel":
                                cancelled = True
                                stop()
                                return
                    finally:
                        if not detached and child.returncode is None:
                            stop()

                async def pump(source, sink, kind):
                    while data := await source.read(65536):
                        target = outputs.get(kind)
                        if target is not None:
                            target.write(data)
                            target.flush()
                        if sink is None:
                            await write_pipe(descriptors[kind], data)
                        else:
                            pending = sink.write(data)
                            if inspect.isawaitable(pending):
                                await pending
                            sink.flush()

                async def feed_input():
                    try:
                        while True:
                            data = forwarded_input.read(65536)
                            if inspect.isawaitable(data):
                                data = await data
                            if not data:
                                break
                            child.stdin.write(data)
                            await child.stdin.drain()
                    except (BrokenPipeError, ConnectionResetError):
                        pass  # A calculation may finish without consuming all stdin.
                    finally:
                        child.stdin.close()

                session_id = uuid4().hex
                launch = asyncio.create_task(asyncio.create_subprocess_exec(
                    *request["argv"], cwd=request["directory"], stdin=input_stream,
                    stdout=outputs.get("out", asyncio.subprocess.DEVNULL) if detached else asyncio.subprocess.PIPE,
                    stderr=outputs.get("err", asyncio.subprocess.DEVNULL) if detached else asyncio.subprocess.PIPE,
                    start_new_session=True,
                ))
                try:
                    try:
                        child = await asyncio.shield(launch)
                    except asyncio.CancelledError:
                        # Reclaim a child created concurrently with cancellation.
                        child = await launch
                        stop()
                        raise
                    identity = session_id
                    # A verified Linux process-group leader can be cancelled
                    # after the original SSH/client session has disappeared.
                    # Do not require KKRunner or psutil on the target host.
                    try:
                        with open(f"/proc/{child.pid}/stat", encoding="ascii") as source:
                            fields = source.read().rsplit(")", 1)[1].split()
                        with open("/proc/sys/kernel/random/boot_id", encoding="ascii") as source:
                            boot_id = source.read().strip()
                        from uuid import UUID
                        if (
                            int(fields[2]) == child.pid
                            and int(fields[3]) == child.pid
                            and fields[0] not in ("Z", "X", "x")
                            and str(UUID(boot_id)) == boot_id
                        ):
                            identity = f"direct:{child.pid}:{int(fields[19])}:{boot_id}"
                    except (OSError, IndexError, ValueError):
                        pass  # Retain an opaque live-session reference.
                    if identity == session_id and sys.platform == "darwin":
                        # There is no /proc on macOS. This start-time identity
                        # permits observation, not cancellation by an old client.
                        import subprocess
                        try:
                            probe = subprocess.run(
                                ("ps", "-p", str(child.pid), "-o", "lstart=", "-o", "pgid="),
                                capture_output=True, text=True, check=True,
                                env={**os.environ, "LC_ALL": "C"},
                            )
                            fields = probe.stdout.split()
                            if len(fields) >= 6 and int(fields[-1]) == child.pid:
                                started_at = "_".join(fields[:-1]).replace(":", "-")
                                identity = f"ps:{child.pid}:{started_at}:{child.pid}"
                        except (OSError, ValueError, subprocess.CalledProcessError):
                            pass
                    if started is not None:
                        notification = started(outcome("running"))
                        if inspect.isawaitable(notification):
                            await notification
                    if control is None:
                        control = asyncio.StreamReader()
                        protocol = asyncio.StreamReaderProtocol(control)
                        pipe = resources.enter_context(os.fdopen(os.dup(sys.stdin.fileno()), "rb", buffering=0))
                        control_transport, _ = await asyncio.get_running_loop().connect_read_pipe(lambda: protocol, pipe)
                    tasks.append(asyncio.create_task(read_control()))
                    input_task = None
                    if forwarded_input is not None:
                        input_task = asyncio.create_task(feed_input())
                        tasks.append(input_task)
                    output_tasks = []
                    if not detached:
                        output_tasks = [
                            asyncio.create_task(pump(child.stdout, stdout, "out")),
                            asyncio.create_task(pump(child.stderr, stderr, "err")),
                        ]
                        tasks.extend(output_tasks)
                    # Control may stay open after completion. It is not part of
                    # the completion barrier; output pipes are drained before return.
                    code, *_ = await asyncio.gather(child.wait(), *output_tasks)
                    if tasks[0].done():
                        tasks[0].result()
                    if input_task is not None and input_task.done():
                        input_task.result()
                    return code
                except BaseException:
                    if child is not None and not detached:
                        stop()
                    raise
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    if child is not None and child.returncode is None and not detached:
                        stop()
                    if stopping is not None and child is not None:
                        await stopping
                        await child.wait()
                    if control_transport is not None:
                        control_transport.close()

        try:
            code = await execute()
        except Exception as exc:
            if identity is not None and detached:
                return outcome("unknown", error=str(exc))
            return outcome("failed", phase="launch" if identity is None else "execution", error=str(exc))
        if cancelled:
            return outcome("cancelled", code)
        if code == 0:
            return outcome("finished", code)
        return outcome("failed", code, phase="execution", error=f"Process exited with status {code}")
