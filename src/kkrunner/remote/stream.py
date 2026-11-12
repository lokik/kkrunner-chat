"""Framing for calculation stdout/stderr over one transport data channel.

The protocol preserves stdout/stderr identity while the streams remain
separate. If a caller explicitly requests a merged sink (``error_file=True``
by the caller), the merge is intentionally lossy: all data is retained in
the output, but cross-stream ordering is not guaranteed to be reproducible and
the original channel identity cannot be recovered afterwards.
"""

from __future__ import annotations

import asyncio
import json

from kkrunner.execution import RunState

def stream_protocol():
    """Self-contained wire encoder, also delivered to targets without KKRunner."""
    import json
    import struct

    class Protocol:
        magic = b"A2SSTRM1"
        header = struct.Struct("!cQ")
        codes = {"out": b"O", "err": b"E", "started": b"S", "terminal": b"T"}

        @classmethod
        def encode(cls, kind, data):
            if kind in ("started", "terminal"):
                data = json.dumps(data, separators=(",", ":"), allow_nan=False).encode("utf8")
            else:
                data = bytes(data)
            return cls.header.pack(cls.codes[kind], len(data)) + data

    return Protocol


_PROTOCOL = stream_protocol()
STREAM_MAGIC = _PROTOCOL.magic
_HEADER = _PROTOCOL.header
_CODE_TO_KIND = {value: key for key, value in _PROTOCOL.codes.items()}


class FramedSink:
    """Write one calculation channel into the framed data stream."""

    def __init__(self, stream, kind):
        self.stream = stream
        self.kind = kind

    def write(self, data):
        self.stream.write(encode_stream_frame(self.kind, data))

    def flush(self):
        self.stream.flush()


async def follow_framed(follow, job_ref, **options):
    """Deliver backend FOLLOW and its terminal outcome over one data channel.

    Transport stderr is reserved for SSH diagnostics. No client file state
    or native process lifetime is owned here.
    """
    import sys

    output = sys.stdout.buffer
    output.write(STREAM_MAGIC)
    output.flush()
    outcome = await follow(
        job_ref,
        FramedSink(output, "out"),
        FramedSink(output, "err"),
        **options,
    )
    output.write(encode_terminal_frame(outcome))
    output.flush()


class StreamProtocolError(RuntimeError):
    """Malformed or unsupported remote stream protocol data."""


def encode_stream_frame(kind, data):
    if kind not in ("out", "err"):
        raise ValueError(f"Unknown stream kind: {kind!r}")
    return _PROTOCOL.encode(kind, data)


def encode_terminal_frame(outcome):
    return _PROTOCOL.encode("terminal", outcome)


async def _read_exactly(source, size):
    try:
        return await source.readexactly(size)
    except asyncio.IncompleteReadError as exc:
        raise StreamProtocolError("Remote stream ended in the middle of a frame") from exc


async def read_stream_frames(source):
    """Yield ``(kind, data)`` records from a framed remote stream."""
    magic = await _read_exactly(source, len(STREAM_MAGIC))
    if magic != STREAM_MAGIC:
        raise StreamProtocolError("Remote stream has an unknown protocol header")

    while True:
        first = await source.read(1)
        if not first:
            return
        rest = await _read_exactly(source, _HEADER.size - 1)
        code, size = _HEADER.unpack(first + rest)
        if code in (b"T", b"S"):
            if size > 4096:
                raise StreamProtocolError("Status frame is too large")
            try:
                outcome = json.loads(await _read_exactly(source, size))
            except (ValueError, UnicodeError) as exc:
                raise StreamProtocolError("Invalid status frame JSON") from exc
            if not isinstance(outcome, dict):
                raise StreamProtocolError("Invalid terminal frame outcome")
            try:
                RunState(outcome.get("state"))
            except (TypeError, ValueError) as exc:
                raise StreamProtocolError("Invalid terminal frame outcome") from exc
            yield _CODE_TO_KIND[code], outcome
            if code == b"S":
                continue
            # The terminal receipt ends the framed stream. Reading through
            # EOF also prevents a misbehaving helper from blocking on a full
            # pipe after sending the terminal frame.
            if await source.read(1):
                raise StreamProtocolError("Remote stream contains data after terminal frame")
            return
        if size > 65536:
            raise StreamProtocolError("Stream frame is too large")
        try:
            kind = _CODE_TO_KIND[code]
        except KeyError as exc:
            raise StreamProtocolError(f"Remote stream contains an unknown channel code: {code!r}") from exc
        yield kind, await _read_exactly(source, size)
