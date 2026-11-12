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
import struct

from kkrunner.execution import RunState

STREAM_MAGIC = b"A2SSTRM1"
_HEADER = struct.Struct("!cQ")
_KIND_TO_CODE = {"out": b"O", "err": b"E"}
_CODE_TO_KIND = {value: key for key, value in _KIND_TO_CODE.items()}


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
    try:
        code = _KIND_TO_CODE[kind]
    except KeyError as exc:
        raise ValueError(f"Unknown stream kind: {kind!r}") from exc
    data = bytes(data)
    return _HEADER.pack(code, len(data)) + data


def encode_terminal_frame(outcome):
    data = json.dumps(outcome, separators=(",", ":")).encode("utf8")
    return _HEADER.pack(b"T", len(data)) + data


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
        if code == b"T":
            if size > 4096:
                raise StreamProtocolError("Terminal frame is too large")
            try:
                outcome = json.loads(await _read_exactly(source, size))
            except (ValueError, UnicodeError) as exc:
                raise StreamProtocolError("Invalid terminal frame JSON") from exc
            if not isinstance(outcome, dict):
                raise StreamProtocolError("Invalid terminal frame outcome")
            try:
                RunState(outcome.get("state"))
            except (TypeError, ValueError) as exc:
                raise StreamProtocolError("Invalid terminal frame outcome") from exc
            yield "terminal", outcome
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
