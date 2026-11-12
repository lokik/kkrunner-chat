"""One portable process implementation, called locally or delivered over SSH."""

from __future__ import annotations

from typing import TYPE_CHECKING

from kkrunner.backends.contracts import Backend, DirectOperations
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
        Acceptance and completion carry an opaque session identity and native
        status. The identity is not a PID and provides no recovery guarantee.
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
