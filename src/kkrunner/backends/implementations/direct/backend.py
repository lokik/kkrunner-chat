"""One portable process implementation, called locally or delivered over SSH."""

from __future__ import annotations

from typing import TYPE_CHECKING

from kkrunner.backends.contracts import Backend, BackendCapability, DirectOperations
from kkrunner.backends.support.process import prepare_mpi
from kkrunner.files.availability import prepare_target_outputs
from kkrunner.files.streams import open_new_output
from kkrunner.support.decorators import depends_on, portable
from kkrunner.support.growing_file_tail import controlled_named_files, observe_file_subscription

from .process import observe_process, wait_detached_process

if TYPE_CHECKING:
    from typing import Awaitable, BinaryIO, Callable, Mapping, Optional, Union
    from kkrunner.backends.contracts import AsyncByteSink, ByteSink, ControlReader
    from kkrunner.backends.support.messages import ExecutionRequest, ExecutionReply


class DirectBackend(Backend, DirectOperations):
    """Session-owned execution without a supervisor or durable outcome receipt."""

    name = "direct"
    prepare_mpi = staticmethod(prepare_mpi)
    prepare_target_outputs = staticmethod(prepare_target_outputs)

    _supports = frozenset({BackendCapability.CANCEL, BackendCapability.STATUS, BackendCapability.REATTACH, BackendCapability.FLEXIBLE_DETACHMENT})

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
    @depends_on(observe_process, controlled_named_files)
    async def follow(
        remote_id, stdout, stderr, *,
        stdout_offset=0, stderr_offset=0, poll_interval=0.2,
        stdout_path=None, stderr_path=None, started=None, output=True, control=None,
    ):
        """Observe saved direct output without owning or restarting the job.

        The operation is source-portable to SSH hosts without kkrunner. A
        legacy opaque ID cannot prove liveness and returns UNKNOWN.
        Cancelling this observer never signals the calculation.
        """
        if type(poll_interval) not in (int, float) or not 0.05 <= poll_interval <= 10:
            raise ValueError("Invalid follow interval")
        offsets = {"out": stdout_offset, "err": stderr_offset}
        paths = {"out": stdout_path, "err": stderr_path}
        sinks = {"out": stdout, "err": stderr}
        for kind, offset in offsets.items():
            if offset is not None and (type(offset) is not int or offset < 0):
                raise ValueError(f"Invalid {kind} output offset")
        def terminal():
            state, message = observe_process(remote_id)
            if state != "running":
                return {"state": state, "return_code": None,
                        "failure_phase": None, "error": message}

        return await controlled_named_files(
            paths, sinks, offsets, terminal, poll_interval, output=output, control=control,
        )

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
    @depends_on(observe_file_subscription, open_new_output, wait_detached_process)
    async def stream(
        request: ExecutionRequest, *,
        stdout: Union[ByteSink, AsyncByteSink],
        stderr: Union[ByteSink, AsyncByteSink],
        control: ControlReader,
        files: Optional[Mapping[str, BinaryIO]] = None,
        started: Optional[Callable[[ExecutionReply], Union[None, Awaitable[None]]]] = None,
        stdin: Optional[BinaryIO] = None,
        output: bool = True,
    ) -> ExecutionReply:
        """Run a process with streams and control supplied by its caller.

        The operation owns the child, its pipe readers and group termination.
        Borrowed sinks may implement synchronous or asynchronous write(). The
        control reader supplies lines: ``cancel`` terminates the process group;
        EOF terminates the child only for attached execution. For a detached
        execution it ends observation; "follow" starts optional named-file delivery.
        Calculation stdin is separate.
        Acceptance and completion carry a session identity and native status.
        On Linux, the identity includes PID/start ticks/boot ID for safe later
        cancellation; elsewhere it stays opaque and cannot be used to kill.
        """
        import asyncio
        import inspect
        import os
        import signal
        import subprocess
        import sys
        from uuid import uuid4
        from contextlib import ExitStack

        detached = bool(request.get("detached", False))
        identity = None
        cancelled = False
        observation_error = None
        output_drained = False

        def outcome(state, code=None, *, phase=None, error=None):
            return {
                "state": state, "return_code": code, "failure_phase": phase,
                "error": error, "remote_id": identity,
            }

        async def execute():
            nonlocal identity, cancelled, observation_error, output_drained
            child = stopping = None
            completed = False
            tasks = []
            with ExitStack() as resources:
                stdin_path = request.get("stdin")
                input_stream = stdin if stdin is not None else (
                    resources.enter_context(open(stdin_path, "rb")) if stdin_path else asyncio.subprocess.DEVNULL
                )
                forwarded_input = None
                if hasattr(input_stream, "read"):
                    try:
                        input_stream.fileno()
                    except (AttributeError, OSError, ValueError):
                        if detached:
                            raise ValueError("Detached stdin requires a file descriptor")
                        forwarded_input, input_stream = input_stream, asyncio.subprocess.PIPE
                outputs = dict(files or {})
                opened = {}
                for kind, path in (("out", request.get("output_file")), ("err", request.get("error_file"))):
                    if kind not in outputs and path is not None:
                        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
                        if path not in opened:
                            opened[path] = resources.enter_context(open_new_output(path, buffering=0))
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

                def cancel():
                    nonlocal cancelled
                    cancelled = True
                    stop()

                async def read_control():
                    # Only attached Direct owns native pipes and lifetime.
                    try:
                        while line := await control.readline():
                            if line.strip() == b"cancel":
                                cancel()
                                return
                    finally:
                        if child.returncode is None:
                            stop()

                async def wait_native():
                    nonlocal completed
                    code = (await wait_detached_process(child) if detached
                            else await child.wait())
                    if stopping is not None:
                        await stopping
                    completed = True
                    return code

                async def pump(source, sink, kind):
                    while data := await source.read(65536):
                        target = outputs.get(kind)
                        if target is not None:
                            target.write(data)
                            target.flush()
                        pending = sink.write(data)
                        if inspect.isawaitable(pending):
                            await pending
                        sink.flush()

                async def feed_input():
                    # Only attached execution can depend on the caller's feeder.
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
                try:
                    if detached:
                        # Popen does not kill a running child when the caller
                        # discards its handle or shuts down its event loop.
                        child = subprocess.Popen(
                            request["argv"], cwd=request["directory"], stdin=input_stream,
                            stdout=outputs.get("out", subprocess.DEVNULL),
                            stderr=outputs.get("err", subprocess.DEVNULL),
                            start_new_session=True,
                        )
                    else:
                        launch = asyncio.create_task(asyncio.create_subprocess_exec(
                            *request["argv"], cwd=request["directory"], stdin=input_stream,
                            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                            start_new_session=True,
                        ))
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
                            and str(UUID(boot_id)) == boot_id
                        ):
                            identity = f"direct:{child.pid}:{int(fields[19])}:{boot_id}"
                    except (OSError, IndexError, ValueError):
                        pass  # Retain an opaque live-session reference.
                    if identity == session_id and sys.platform == "darwin":
                        # There is no /proc on macOS. This start-time identity
                        # permits observation, not cancellation by an old client.
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
                    input_task = control_task = None
                    if forwarded_input is not None:
                        input_task = asyncio.create_task(feed_input())
                        tasks.append(input_task)
                    if detached:
                        code, output_drained, observation_error = await observe_file_subscription(
                            wait_native(),
                            {"out": request.get("output_file"), "err": request.get("error_file")},
                            {"out": stdout, "err": stderr}, {"out": 0, "err": 0}, control,
                            output=output, poll_interval=0.2, cancel=cancel,
                        )
                    else:
                        control_task = asyncio.create_task(read_control())
                        output_tasks = [
                            asyncio.create_task(pump(child.stdout, stdout, "out")),
                            asyncio.create_task(pump(child.stderr, stderr, "err")),
                        ]
                        tasks.extend((control_task, *output_tasks))
                        # Native attached pipes must be drained before returning.
                        code, *_ = await asyncio.gather(wait_native(), *output_tasks)
                        if stopping is not None:
                            await stopping
                        if control_task.done():
                            control_task.result()
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
                    if stopping is not None and child is not None and not completed:
                        await stopping
                        await wait_native()

        try:
            code = await execute()
        except Exception as exc:
            if identity is not None and detached:
                return outcome("unknown", error=str(exc))
            return outcome("failed", phase="launch" if identity is None else "execution", error=str(exc))
        if cancelled:
            facts = outcome("cancelled", code)
        elif code == 0:
            facts = outcome("finished", code)
        else:
            facts = outcome("failed", code, phase="execution", error=f"Process exited with status {code}")
        if detached:
            facts["output_drained"] = output_drained
            if observation_error is not None:
                facts["output_error"] = observation_error
        return facts
