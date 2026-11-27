"""FOLLOW is an observation operation, not a backend-specific wire method."""

import asyncio
import sys

from kkrunner.client.contracts import Operation
from kkrunner.targets import SshTarget
from kkrunner.support.decorators import portable


@portable
async def follow(identity, stdout, stderr, **options):
    await stdout.write(b"saved output")
    await stderr.write(b"diagnostic")
    return {"state": "finished", "return_code": None}


def test_ssh_follow_can_observe_a_portable_operation_without_backend(monkeypatch):
    monkeypatch.setattr(SshTarget, "_command", lambda self, argv, cwd=None: tuple(argv))
    target = SshTarget("loopback", python_command=(sys.executable, "-I", "-S"))

    async def exercise():
        chunks = []
        facts = await target.follow(Operation.standalone(follow), "identity",
                                    lambda data, kind: chunks.append((kind, data)))
        assert facts == {"state": "finished", "return_code": None}
        assert chunks == [("out", b"saved output"), ("err", b"diagnostic")]

    asyncio.run(exercise())
