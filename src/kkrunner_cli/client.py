"""Human-facing client for persisted runs and explicit backend handles.

Only this module understands a local RunStore. The target-side agent has no
run selection, store, or application-specific parser.
"""

import argparse
import json
import sys


def parser(root):
    root.add_argument("--store", help="local RunStore directory for run IDs")
    root.add_argument("--host", help="SSH execution host (required for remote-id over SSH)")
    root.add_argument("--ssh-option", action="append", default=[], help="SSH option, repeatable")
    root.add_argument("--agent", default="kkrunner-agent", help="target-side agent executable")
    actions = root.add_subparsers(dest="action", required=True)
    for name in ("status", "follow", "cancel"):
        action = actions.add_parser(name)
        identity = action.add_mutually_exclusive_group(required=True)
        identity.add_argument("run_id", nargs="?", help="persisted client run ID")
        identity.add_argument("--remote-id", help="opaque backend handle, without a RunStore")
        if name == "follow":
            action.add_argument("--from-start", action="store_true",
                                help="replay old output (default: only new output)")
            action.add_argument("--stdout-path", help="explicit backend stdout file (remote-id only)")
            action.add_argument("--stderr-path", help="explicit backend stderr file (remote-id only)")


def _facts(status):
    return {
        "state": status.state.value,
        "return_code": status.return_code,
        "error": status.message,
        "failure_phase": None if status.failure_phase is None else status.failure_phase.value,
    }


def _record(args):
    if args.store is None:
        raise ValueError("run_id requires --store pointing to the client RunStore")
    from kkrunner.runs.records import RunStore
    return RunStore(args.store).get(args.run_id)


def _direct(args, *, action):
    from kkrunner.remote.service import status_remote, cancel_remote, follow_remote
    if not args.host:
        if action == "status":
            return status_remote(args.remote_id)
        if action == "cancel":
            return cancel_remote(args.remote_id)
        offset = 0 if args.from_start else None
        return follow_remote(args.remote_id, sys.stdout.buffer, sys.stderr.buffer,
                             stdout_offset=offset, stderr_offset=offset,
                             stdout_path=args.stdout_path, stderr_path=args.stderr_path)
    from kkrunner.client.transports import SshTransport
    from kkrunner.remote.protocol import json_request
    transport = SshTransport(args.host, options=args.ssh_option)
    if action != "follow":
        from kkrunner.support.async_utils import run_coro_sync
        return run_coro_sync(json_request(transport, (args.agent,), action, args.remote_id))
    return _follow_remote(transport, (args.agent,), args.remote_id,
                          args.from_start, args.stdout_path, args.stderr_path)


def _follow_remote(transport, command, remote_id, from_start, stdout_path, stderr_path):
    from kkrunner.remote.protocol import control_command
    from kkrunner.client.transports.follow import follow_framed_command
    from kkrunner.support.async_utils import run_coro_sync

    async def observe():
        options = []
        if stdout_path:
            options.extend(("--stdout-path", stdout_path))
        if stderr_path:
            options.extend(("--stderr-path", stderr_path))
        if from_start:
            options.extend(("--stdout-offset", "0", "--stderr-offset", "0"))
        else:
            options.extend(("--live-stdout", "--live-stderr"))
        proc = await transport.start(control_command(command, "follow", remote_id, *options, "--framed"))
        def emit(data, kind):
            output = sys.stderr.buffer if kind == "err" else sys.stdout.buffer
            output.write(data)
            output.flush()

        return await follow_framed_command(proc, emit)

    return run_coro_sync(observe())


def _persisted(args):
    from kkrunner.client.contracts import JobAttachment
    from kkrunner.client import EndpointClient
    record = _record(args)
    if args.host:
        raise ValueError("--host is only for --remote-id; persisted runs use their recorded transport")
    if args.action == "follow" and (args.stdout_path or args.stderr_path):
        raise ValueError("Explicit stream paths require --remote-id")
    if record.state.is_terminal:
        if args.action == "follow":
            if args.from_start:
                seen = set()
                for entry, output in ((record.files.stdout, sys.stdout.buffer),
                                      (record.files.stderr, sys.stderr.buffer)):
                    if entry is not None and entry.local_path and entry.local_path not in seen:
                        seen.add(entry.local_path)
                        with open(entry.local_path, "rb") as source:
                            while data := source.read(65536):
                                output.write(data)
                                output.flush()
        elif args.action == "cancel":
            return {"state": record.state.value, "return_code": record.return_code,
                    "error": record.message}
        return {"state": record.state.value, "return_code": record.return_code,
                "error": record.message}
    if record.execution.backend_id is None:
        if args.action == "follow" or args.action == "cancel":
            raise RuntimeError("Run has not been submitted")
        return {"state": record.state.value, "return_code": record.return_code,
                "error": record.message}
    client = EndpointClient.from_config(record.execution.transport, record.execution.backend)
    attachment = JobAttachment(record.execution, record.files)
    if args.action == "status":
        return _facts(client.status(attachment)[0])
    from kkrunner.support.async_utils import run_coro_sync
    if args.action == "cancel":
        return run_coro_sync(client._cancel(record.execution.directory.target,
                                           record.execution.backend_id))
    offset = 0 if args.from_start else None
    def output(data, kind):
        stream = sys.stderr.buffer if kind == "err" else sys.stdout.buffer
        stream.write(data)
        stream.flush()
    remote = not client.transport.is_local
    def path(entry):
        return None if entry is None else entry.remote_path if remote else entry.local_path
    return run_coro_sync(client._follow(
        record.execution.directory.target, record.execution.backend_id,
        on_output=output, offsets={"out": offset, "err": offset},
        stdout_path=path(record.files.stdout), stderr_path=path(record.files.stderr),
    ))


def run(args):
    if args.remote_id:
        if args.store:
            raise ValueError("--store cannot be combined with --remote-id")
        if args.action == "follow":
            result = _direct(args, action="follow")
        else:
            result = _direct(args, action=args.action)
    else:
        result = _persisted(args)
    if args.action != "follow":
        print(json.dumps(result), flush=True)
    return result


def main(argv=None):
    root = argparse.ArgumentParser(prog="kkrunner", description="Inspect, follow, and cancel execution runs.")
    parser(root)
    args = root.parse_args(argv)
    try:
        return run(args)
    except (ValueError, FileNotFoundError) as exc:
        root.error(str(exc))


if __name__ == "__main__":
    main()
