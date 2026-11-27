"""Supervisor child execution lifecycle."""

import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
from pathlib import Path

from kkrunner.backends.support.process import pump_stream, stop_local_process
from kkrunner.backends.implementations.supervised.streams import SupervisorStreams
from kkrunner.backends.implementations.supervised.mailbox import publish_outcome
from kkrunner.execution import FailurePhase, RunState

_SPEC_ENV = "KKRunner_REMOTE_PROCESS_SPEC"
_START_FD_ENV = "KKRunner_REMOTE_PROCESS_START_FD"
_LAUNCH_FAILURE_RETURN_CODE = 127


def _exception_message(exc):
    """Include explicitly chained failures in the persisted run outcome."""
    message = str(exc)
    if exc.__cause__ is not None:
        message += f"; secondary error: {exc.__cause__}"
    return message


def _wait_for_parent_release():
    value = os.environ.pop(_START_FD_ENV, None)
    if value is None:
        return
    fd = int(value)
    try:
        if os.read(fd, 1) != b"1":
            raise RuntimeError("Remote supervisor launch was not released by parent")
    finally:
        os.close(fd)


async def _start_process(spec, stdin, streams, exited):
    """Observe process exit independently of subprocess pipe closure."""
    loop = asyncio.get_running_loop()

    class ExitProtocol(asyncio.subprocess.SubprocessStreamProtocol):
        def process_exited(self):
            super().process_exited()
            exited.set_result(None)

    transport, protocol = await loop.subprocess_exec(
        lambda: ExitProtocol(limit=65536, loop=loop),
        *spec["argv"],
        cwd=spec["directory"],
        stdin=stdin,
        stdout=asyncio.subprocess.PIPE,
        stderr=(asyncio.subprocess.STDOUT if streams.merged else asyncio.subprocess.PIPE),
        start_new_session=True,
    )
    return asyncio.subprocess.Process(transport, protocol, loop)


def _set_active_state(active, state):
    """Change the transient execution state without a gap in status queries."""
    temporary = active.with_suffix(".next")
    temporary.write_text(state.value)
    temporary.replace(active)


async def _run(spec):
    streams = SupervisorStreams(
        spec.get("output_file"),
        spec.get("error_file"),
        detached=spec.get("detached", True),
        socket_dir=spec.get("socket_dir"),
    )
    state, phase, return_code, message = RunState.FAILED, FailurePhase.LAUNCH, 127, None
    opened = []
    process = None
    streams_started = False
    initial_socket = None
    active = None
    exited = asyncio.get_running_loop().create_future()
    try:
        try:
            if "initial_follower_fd" in spec:
                initial_socket = socket.socket(fileno=spec["initial_follower_fd"])
            elif not streams.detached:
                raise ValueError("Attached supervisor requires an initial observer")
            await streams.start()
            streams_started = True
            active = Path(streams.socket_path).with_name("active")
            _set_active_state(active, RunState.RUNNING)
            if initial_socket is not None:
                await streams.attach_initial(initial_socket, output=spec.get("initial_output", True))
                initial_socket = None  # The asyncio stream now owns the socket.
            os.write(int(spec["socket_fd"]), (streams.socket_path + "\n").encode())
        finally:
            # Closing the handshake FD also wakes a waiting parent when the
            # supervisor cannot create its FOLLOW socket.
            os.close(int(spec["socket_fd"]))
        if streams.cancelled.is_set() or (not streams.detached and streams.disconnected.is_set()):
            state, phase, return_code = RunState.CANCELLED, None, -signal.SIGTERM
            return return_code
        stdin = subprocess.DEVNULL
        if spec.get("stdin"):
            stdin = open(spec["stdin"], "rb")
            opened.append(stdin)
        process = await _start_process(spec, stdin, streams, exited)
        state, phase, return_code = RunState.RUNNING, None, None
        output_error = asyncio.get_running_loop().create_future()

        async def pump(source, kind):
            try:
                await pump_stream(source, lambda data: streams.publish(kind, data))
            except Exception as exc:
                if not output_error.done():
                    output_error.set_result(exc)
                # The worker must be able to stop the group even if a failed
                # output sink has left its subprocess pipe full.
                streams.release_backpressure()
                while await source.read(65536):
                    pass

        pumps = [asyncio.create_task(pump(process.stdout, "out"))]
        if not streams.merged:
            pumps.append(asyncio.create_task(pump(process.stderr, "err")))
        async def finish_output():
            await exited
            streams.release_backpressure()
            _set_active_state(active, RunState.FINISHING)
            return await asyncio.gather(*pumps, return_exceptions=True)

        completion = asyncio.create_task(finish_output())
        stop = asyncio.create_task(streams.cancelled.wait())
        interrupts = [output_error, stop]
        if not streams.detached:
            interrupts.append(asyncio.create_task(streams.disconnected.wait()))
        try:
            done, _ = await asyncio.wait(
                [completion, *interrupts], return_when=asyncio.FIRST_COMPLETED,
            )
            if completion not in done:
                # The same transition covers cancellation, observer loss and
                # output failure. Release blocked readers before stopping.
                streams.release_backpressure()
                if not exited.done() and not output_error.done():
                    state, phase = RunState.CANCELLED, None
                await stop_local_process(process, process_group=True, timeout=1.0, waiting=exited)
            pump_results = await completion
            return_code = process.returncode
            pump_error = (
                output_error.result() if output_error.done() else
                next((value for value in pump_results if isinstance(value, BaseException)), None)
            )
            if pump_error is not None:
                state, phase = RunState.FAILED, FailurePhase.EXECUTION
                message = f"Failed to write process output: {pump_error}"
            elif state == RunState.RUNNING:
                state = RunState.FINISHED if return_code == 0 else RunState.FAILED
                phase = None if return_code == 0 else FailurePhase.EXECUTION
        finally:
            tasks = [completion, *interrupts, *pumps]
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    except OSError as exc:
        message = _exception_message(exc)
        state, phase = RunState.FAILED, (FailurePhase.LAUNCH if process is None else FailurePhase.EXECUTION)
    except Exception as exc:
        message = _exception_message(exc)
        state, phase = RunState.FAILED, (FailurePhase.LAUNCH if process is None else FailurePhase.EXECUTION)
        if process is not None and process.returncode is None:
            streams.release_backpressure()
            await stop_local_process(process, process_group=True, timeout=1.0, waiting=exited)
        return_code = process.returncode if process is not None else 127
    finally:
        if initial_socket is not None:
            initial_socket.close()
        for f in opened:
            try:
                f.close()
            except OSError as exc:
                # Failure to close stdin must not suppress the primary launch
                # error or prevent the durable terminal outcome from being
                # published. On an otherwise successful run it is a failure.
                if state == RunState.FINISHED:
                    state, phase = RunState.FAILED, FailurePhase.EXECUTION
                    message = f"Failed to close process input: {exc}"
                elif message is None:
                    message = f"Failed to close process input: {exc}"
        if streams_started:
            try:
                streams.sync()
            except OSError as exc:
                state, phase = RunState.FAILED, FailurePhase.ARTIFACTS
                message = f"Failed to sync process output: {exc}"
        outcome = {
            "state": state.value,
            "return_code": return_code,
            "failure_phase": None if phase is None else phase.value,
            "error": message,
        }
        try:
            publish_outcome(
                spec["receipt_id"],
                state,
                return_code,
                phase,
                error=message,
            )
        except OSError as exc:
            # No durable terminal receipt exists. The client must preserve its
            # active run and retry recovery, never commit FINISHED from a socket.
            outcome = {
                "state": RunState.UNKNOWN.value,
                "return_code": None,
                "failure_phase": None,
                "error": f"Unable to persist terminal receipt: {exc}",
            }
        finally:
            # start() already attempted cleanup if startup failed. There is
            # no server to notify in that case; avoid a second failing cleanup.
            if streams_started:
                if active is not None:
                    active.unlink(missing_ok=True)
                streams.terminal(outcome)
                await streams.finish()
    return return_code if return_code is not None else 127


def main():
    raw = os.environ.pop(_SPEC_ENV, None)
    if raw is None:
        raise RuntimeError(f"Missing {_SPEC_ENV}")
    spec = json.loads(raw)
    _wait_for_parent_release()
    return asyncio.run(_run(spec))





if __name__ == '__main__':
    sys.exit(main())
