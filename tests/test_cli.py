import argparse
import io
import json
from types import SimpleNamespace

import pytest

import kkrunner.remote.service as remote_service
from kkrunner_cli import main as remote_command
from kkrunner.remote.protocol import CONTROL_PROTOCOL_VERSION


def remote_args(**kwargs):
    return SimpleNamespace(**({"protocol_version": CONTROL_PROTOCOL_VERSION} | kwargs))


def test_status_command_forwards_opaque_remote_handle(monkeypatch):
    remote_id = "opaque-handle"
    calls = []
    monkeypatch.setattr(
        remote_service,
        "status_remote",
        lambda value: calls.append(value) or {"state": "running"},
    )
    output = []
    monkeypatch.setattr(remote_command, "_print_json", output.append)

    remote_command.run(
        remote_args(remote_action="status", remote_id=remote_id),
        None,
    )

    assert calls == [remote_id]
    assert output == [{"state": "running"}]


def test_submit_command_passes_complete_request_to_stateless_service(monkeypatch):
    calls = []
    monkeypatch.setattr(
        remote_service,
        "submit_remote",
        lambda request: calls.append(dict(request)) or {"remote_id": "opaque", "state": "running"},
    )
    monkeypatch.setattr(
        remote_command.sys,
        "stdin",
        io.StringIO(json.dumps({"id": "run-1", "argv": ["true"]})),
    )
    output = []
    monkeypatch.setattr(remote_command, "_print_json", output.append)

    remote_command.run(
        remote_args(remote_action="submit", directory="/remote/work"),
        None,
    )

    assert calls == [{"id": "run-1", "argv": ["true"], "directory": "/remote/work"}]
    assert output == [{"remote_id": "opaque", "state": "running"}]


def test_resolve_mpi_command_preserves_integer_process_count(monkeypatch):
    monkeypatch.delenv("SLURM_NODELIST", raising=False)
    monkeypatch.setattr("shutil.which", lambda name: "/usr/bin/mpirun" if name == "mpirun" else None)
    monkeypatch.setattr(remote_command.sys, "stdin", io.StringIO('{"mpi": 4}'))
    output = []
    monkeypatch.setattr(remote_command, "_print_json", output.append)

    remote_command.run(remote_args(remote_action="resolve-mpi"), None)

    assert output == [{"mpi": ("mpirun", "-np", "4")}]


@pytest.mark.parametrize("action", ["submit", "query-files", "finalize-upload"])
def test_incompatible_protocol_rejected_before_request_is_read(monkeypatch, action):
    monkeypatch.setattr(remote_command.sys, "stdin", io.StringIO("not JSON"))
    args = SimpleNamespace(remote_action=action, protocol_version=CONTROL_PROTOCOL_VERSION + 1)
    with pytest.raises(ValueError, match="Unsupported remote control protocol"):
        remote_command.run(args, None)


def test_agent_requires_protocol_version():
    parser = argparse.ArgumentParser()
    remote_command.parser(parser)
    with pytest.raises(SystemExit):
        parser.parse_args(["status", "opaque"])
    parsed = parser.parse_args(["--protocol-version", str(CONTROL_PROTOCOL_VERSION), "status", "opaque"])
    assert parsed.protocol_version == CONTROL_PROTOCOL_VERSION


def test_help_does_not_import_kkrunner():
    import subprocess
    import sys

    code = """\
import sys
from kkrunner_cli.main import main
try:
    main(["--help"])
except SystemExit as exc:
    assert exc.code == 0
else:
    raise AssertionError("help should exit")
assert not any(name == "kkrunner" or name.startswith("kkrunner.") for name in sys.modules)
"""
    subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True)


def test_agent_exposes_only_protocol_actions():
    parser = argparse.ArgumentParser()
    remote_command.parser(parser)
    parsed = parser.parse_args(["--protocol-version", "1", "follow", "opaque", "--live"])
    assert parsed.live is True
    with pytest.raises(SystemExit):
        parser.parse_args(["--protocol-version", "1", "unknown"])
    assert not hasattr(remote_command, "user_parser")



def test_file_query_cli_does_not_import_remote_execution():
    import subprocess
    import sys

    code = """\
import io
import sys
sys.stdin = io.StringIO('{"paths": []}')
from kkrunner_cli.main import main
main(["--protocol-version", "1", "query-files"])
assert "kkrunner.remote.service" not in sys.modules
assert "kkrunner.remote.executor" not in sys.modules
assert "kkrunner.backends.pool" not in sys.modules
assert "kkrunner.runs.manager" not in sys.modules
"""
    proc = subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True)
    assert json.loads(proc.stdout) == []

def test_client_parser_uses_persisted_run_id_or_explicit_handle():
    from kkrunner_cli.client import parser
    root = argparse.ArgumentParser()
    parser(root)
    parsed = root.parse_args(["--store", "/tmp/runs", "status", "run-abc"])
    assert parsed.run_id == "run-abc"
    assert parsed.remote_id is None
    parsed = root.parse_args(["--host", "cluster", "follow", "--remote-id", "opaque", "--from-start"])
    assert parsed.remote_id == "opaque"
    assert parsed.from_start
    with pytest.raises(SystemExit):
        root.parse_args(["status", "run-abc", "--remote-id", "opaque"])


def test_client_status_from_persisted_record(monkeypatch, capsys):
    from kkrunner_cli import client
    from kkrunner.runs.base import RunState
    record = SimpleNamespace(id="run-abc", state=RunState.FINISHED,
                             return_code=0, message=None)
    monkeypatch.setattr(client, "_record", lambda args: record)
    client.main(["--store", "/runs", "status", "run-abc"])
    assert json.loads(capsys.readouterr().out) == {
        "state": "finished", "return_code": 0, "error": None,
    }


def test_client_direct_handle_is_not_a_run_id(monkeypatch, capsys):
    from kkrunner_cli import client
    monkeypatch.setattr(client, "_direct", lambda args, *, action: {
        "state": "running", "action": action, "id": args.remote_id,
    })
    client.main(["status", "--remote-id", "opaque"])
    assert json.loads(capsys.readouterr().out) == {
        "state": "running", "action": "status", "id": "opaque",
    }


def test_client_help_is_lightweight():
    import subprocess
    import sys
    code = """
import sys
from kkrunner_cli.client import main
try:
    main(["--help"])
except SystemExit as exc:
    assert exc.code == 0
else:
    raise AssertionError("help should exit")
assert not any(name == "kkrunner" or name.startswith("kkrunner.") for name in sys.modules)
"""
    subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True)
