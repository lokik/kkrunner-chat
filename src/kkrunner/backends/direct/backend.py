"""One portable process implementation, called locally or delivered over SSH."""

from __future__ import annotations

from typing import TYPE_CHECKING

from kkrunner.backends.contracts import Backend, BackendCapability, DirectOperations
from kkrunner.backends.support.process import prepare_mpi
from kkrunner.files.availability import prepare_target_outputs

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
        self.supports = frozenset({BackendCapability.CANCEL, BackendCapability.STATUS})

    @staticmethod
    def status(remote_id: str):
        """Observe a direct process independently of the original session.

        A returned FINISHED means termination was verified, not that the
        calculation succeeded. An unavailable or unverifiable identity stays
        UNKNOWN. Portable SSH execution needs only target-side stdlib.
        """
        import os
        import subprocess
        import sys

        def reply(state, error=None):
            return {
                "state": state, "return_code": None,
                "failure_phase": None, "error": error,
            }

        parts = remote_id.split(":") if isinstance(remote_id, str) else ()
        if len(parts) != 4 or parts[0] not in ("direct", "ps"):
            return reply("unknown", "A verifiable direct process identity is unavailable")
        try:
            pid = int(parts[1])
            if pid <= 0:
                raise ValueError("Invalid PID")
        except (ValueError, TypeError):
            return reply("unknown", "Invalid direct process identity")

        def group_running():
            if sys.platform.startswith("linux"):
                try:
                    with os.scandir("/proc") as entries:
                        for entry in entries:
                            if not entry.name.isdecimal():
                                continue
                            try:
                                with open(entry.path + "/stat", encoding="ascii") as source:
                                    fields = source.read().rsplit(")", 1)[1].split()
                                if int(fields[2]) == pid and fields[0] not in ("Z", "X", "x"):
                                    return True
                            except FileNotFoundError:
                                continue
                            except (PermissionError, OSError, ValueError, IndexError):
                                return None
                except OSError:
                    return None
                return False
            try:
                probe = subprocess.run(
                    ("ps", "-e", "-o", "pgid=,stat="),
                    capture_output=True, text=True, check=True,
                    env={**os.environ, "LC_ALL": "C"},
                )
                for line in probe.stdout.splitlines():
                    fields = line.split()
                    if len(fields) >= 2 and fields[0].isdigit() and int(fields[0]) == pid and fields[1][0] not in ("Z", "X"):
                        return True
                return False
            except (OSError, subprocess.CalledProcessError):
                return None

        if parts[0] == "direct":
            if not sys.platform.startswith("linux"):
                return reply("unknown", "Linux process identity cannot be verified on this host")
            try:
                start = int(parts[2])
                from uuid import UUID
                boot = str(UUID(parts[3]))
                if start < 0 or boot != parts[3]:
                    raise ValueError("Invalid process identity")
            except (TypeError, ValueError, OverflowError):
                return reply("unknown", "Invalid direct process identity")
            try:
                with open("/proc/sys/kernel/random/boot_id", encoding="ascii") as source:
                    current_boot = source.read().strip()
            except OSError as exc:
                return reply("unknown", f"Cannot verify kernel boot: {exc}")
            if current_boot != boot:
                return reply("finished", "Process belongs to an earlier boot; exit code unavailable")
            try:
                with open(f"/proc/{pid}/stat", encoding="ascii") as source:
                    fields = source.read().rsplit(")", 1)[1].split()
                if int(fields[19]) != start:
                    # A reused PID must never be mistaken for the original job.
                    return reply("finished", "Original process identity ended; exit code unavailable")
                if int(fields[2]) != pid or int(fields[3]) != pid:
                    return reply("unknown", "Original process group cannot be verified")
                running = fields[0] not in ("Z", "X", "x")
            except FileNotFoundError:
                running = False
            except (OSError, ValueError, IndexError) as exc:
                return reply("unknown", f"Cannot verify process identity: {exc}")
        else:
            if sys.platform.startswith("linux"):
                return reply("unknown", "Non-Linux process identity cannot be verified on this host")
            try:
                probe = subprocess.run(
                    ("ps", "-p", str(pid), "-o", "lstart=", "-o", "stat="),
                    capture_output=True, text=True,
                    env={**os.environ, "LC_ALL": "C"},
                )
            except OSError as exc:
                return reply("unknown", f"Cannot inspect process identity: {exc}")
            if probe.returncode not in (0, 1):
                return reply("unknown", probe.stderr.strip() or "Process query failed")
            fields = probe.stdout.split()
            if parts[3] != str(pid):
                return reply("unknown", "Invalid process group identity")
            if fields and "_".join(fields[:-1]).replace(":", "-") != parts[2]:
                return reply("finished", "Original process identity ended; exit code unavailable")
            running = bool(fields) and fields[-1][0] not in ("Z", "X")

        if running:
            return reply("running")
        members = group_running()
        if members is True:
            return reply("running")
        if members is None:
            return reply("unknown", "Cannot verify surviving process-group members")
        return reply("finished", "Process ended; exit code unavailable")

    @staticmethod
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

        def observe():
            try:
                with open("/proc/sys/kernel/random/boot_id", encoding="ascii") as source:
                    current_boot = source.read().strip()
            except OSError as exc:
                return "unknown", f"Cannot verify kernel boot identity: {exc}"
            if current_boot != boot:
                return "gone", None
            try:
                with open(f"/proc/{pid}/stat", encoding="ascii") as source:
                    data = source.read()
            except FileNotFoundError:
                return "gone", None
            except OSError as exc:
                return "unknown", f"Cannot verify process identity: {exc}"
            try:
                fields = data.rsplit(")", 1)[1].split()
                state, pgrp, session, start_ticks = (
                    fields[0], int(fields[2]), int(fields[3]), int(fields[19])
                )
            except (IndexError, ValueError) as exc:
                return "unknown", f"Cannot parse Linux process identity: {exc}"
            if start_ticks != started:
                return "gone", None  # The PID was reused; never signal it.
            if state in ("Z", "X", "x"):
                return "gone", None
            if pgrp != pid or session != pid:
                return "unknown", "Saved PID does not identify its original session leader"
            return "running", None

        current, error = observe()
        if current == "gone":
            return reply("finished", "The original process is no longer running; exit status unavailable")
        if current != "running":
            return reply("unknown", error)

        try:
            os.killpg(pid, signal.SIGTERM)
        except ProcessLookupError:
            return reply("finished", "Process exited before cancellation; exit status unavailable")

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            current, error = observe()
            if current != "running":
                break
            time.sleep(min(0.05, max(0, deadline - time.monotonic())))
        else:
            current, error = observe()

        if current == "running":
            # Recheck original identity immediately before escalating. A group
            # with a reused PID or an unverifiable leader must not be signalled.
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                current = "gone"
            else:
                current, error = observe()

        if current == "gone":
            return reply("cancelled")
        return reply(
            "running" if current == "running" else "unknown",
            "Cancellation signal sent; termination not yet verified"
            if current == "running" else error,
        )

    @staticmethod
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
