"""One-shot target-side control agent; no user-facing run selection."""

import json
import sys

help = "Control KKRunner execution on this machine."
description = "Submit, inspect, follow and cancel remote execution handles."


def parser(argument_parser):
    argument_parser.add_argument("--protocol-version", type=int, required=True,
                                help="control protocol version supplied by an execution client")
    actions = argument_parser.add_subparsers(dest="remote_action", required=True)
    submit = actions.add_parser("submit")
    submit.add_argument("directory")
    status = actions.add_parser("status")
    status.add_argument("remote_id")
    cancel = actions.add_parser("cancel")
    cancel.add_argument("remote_id")
    follow = actions.add_parser("follow")
    follow.add_argument("remote_id")
    follow.add_argument("--stdout-path")
    follow.add_argument("--stderr-path")
    follow.add_argument("--live", action="store_true")
    follow.add_argument("--stdout-offset", type=int, default=0)
    follow.add_argument("--stderr-offset", type=int, default=0)
    follow.add_argument("--live-stdout", action="store_true")
    follow.add_argument("--live-stderr", action="store_true")
    follow.add_argument("--framed", action="store_true")
    recover = actions.add_parser("recover")
    recover.add_argument("directory")
    recover.add_argument("run_id")
    ack_submit = actions.add_parser("ack-submit")
    ack_submit.add_argument("backend")
    ack_submit.add_argument("run_id")
    ack = actions.add_parser("ack")
    ack.add_argument("remote_id")
    actions.add_parser("resolve-mpi")
    actions.add_parser("query-files")
    actions.add_parser("finalize-upload")



def _print_json(value):
    print(json.dumps(value), flush=True)



def run(args, global_args=None):
    from kkrunner.remote.protocol import check_control_protocol

    check_control_protocol(args.protocol_version)
    if args.remote_action == "query-files":
        from kkrunner.files.queries import query_files

        request = json.load(sys.stdin)
        _print_json(query_files(request["paths"], root=request.get("root"), digest=request.get("digest", False)))
        return
    if args.remote_action == "finalize-upload":
        from kkrunner.files.queries import finalize_upload

        finalize_upload(**json.load(sys.stdin))
        _print_json({"published": True})
        return
    from kkrunner.remote.service import (
        acknowledge_remote,
        acknowledge_submission,
        cancel_remote,
        follow_remote,
        recover_remote,
        status_remote,
        submit_remote,
    )

    if args.remote_action == "submit":
        request = json.load(sys.stdin)
        request["directory"] = args.directory
        _print_json(submit_remote(request))
    elif args.remote_action == "status":
        _print_json(status_remote(args.remote_id))
    elif args.remote_action == "cancel":
        _print_json(cancel_remote(args.remote_id))
    elif args.remote_action == "follow":
        if args.framed:
            from kkrunner.remote.stream import STREAM_MAGIC

            stream = sys.stdout.buffer
            stream.write(STREAM_MAGIC)
            stream.flush()
            from kkrunner.remote.stream import FramedSink
            stdout = FramedSink(stream, "out")
            stderr = FramedSink(stream, "err")
        else:
            stdout = sys.stdout.buffer
            stderr = sys.stderr.buffer
        outcome = follow_remote(
            args.remote_id,
            stdout,
            stderr,
            stdout_offset=None if args.live or args.live_stdout else args.stdout_offset,
            stderr_offset=None if args.live or args.live_stderr else args.stderr_offset,
            stdout_path=args.stdout_path,
            stderr_path=args.stderr_path,
        )
        if args.framed:
            from kkrunner.remote.stream import encode_terminal_frame

            stream.write(encode_terminal_frame(outcome))
            stream.flush()
    elif args.remote_action == "recover":
        request = json.load(sys.stdin)
        request["directory"] = args.directory
        submitted_at = request.pop("submitted_at", None)
        try:
            result = recover_remote(
                args.run_id,
                request,
                submitted_at=submitted_at,
            )
        except NotImplementedError as exc:
            result = {"unsupported": True, "error": str(exc)}
        _print_json(result)
    elif args.remote_action == "ack-submit":
        _print_json(acknowledge_submission(args.backend, args.run_id))
    elif args.remote_action == "ack":
        _print_json(acknowledge_remote(args.remote_id))
    elif args.remote_action == "resolve-mpi":
        from kkrunner.backends.support.process import resolve_mpi_command
        from kkrunner.execution import MpiSpec

        request = json.load(sys.stdin)
        mpi = MpiSpec.from_value(request.get("mpi"))
        _print_json({"mpi": resolve_mpi_command(mpi)})
    else:
        raise ValueError("Unknown remote action: {}".format(args.remote_action))


def main(argv=None):
    import argparse

    root = argparse.ArgumentParser(prog="kkrunner-agent", description=description)
    parser(root)
    args = root.parse_args(argv)
    run(args)


if __name__ == "__main__":
    main()
