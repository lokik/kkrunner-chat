"""Incremental observation of growing files, shared by local and remote runners.

Like GNU tail -f, use inotify where available and recheck periodically where
filesystem notifications are unsupported or not forwarded (notably some network
filesystems). Ordinary file EOF is not a waitable event, even under asyncio.
"""

from __future__ import annotations

import asyncio
import ctypes
import os
import sys
from pathlib import Path

from kkrunner.support.decorators import depends_on, portable

# Directory watches also detect an output file created after observation starts
# or atomically renamed into place.
_WATCH_MASK = 0x00000002 | 0x00000004 | 0x00000008 | 0x00000080 | 0x00000100 | 0x00000200 | 0x00000400 | 0x00000800


class GrowingFileTail:
    """Read only new bytes, and await changes without busy-polling.

    Opened files belong to logical channels, not pathnames. Renaming or
    replacing a pathname never changes the inode being observed.
    None as an initial offset means start at current EOF (live-only observation).
    """

    def __init__(self, paths, *, offsets=None, watch_paths=(), poll_interval=1.0, chunk_size=65536,
                 sources=None):
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        self.paths = dict(paths)
        self.offsets = offsets if offsets is not None else {kind: 0 for kind in self.paths}
        self.watch_paths = tuple(watch_paths)
        self.poll_interval = poll_interval
        self.chunk_size = chunk_size
        self._owns_sources = sources is None
        self._sources = {} if sources is None else sources
        self._fd = None
        self._loop = None
        self._changed = asyncio.Event()

    async def __aenter__(self):
        self._loop = asyncio.get_running_loop()
        if sys.platform != "linux" or not hasattr(self._loop, "add_reader"):
            return self
        libc = ctypes.CDLL(None, use_errno=True)
        init = getattr(libc, "inotify_init1", None)
        add_watch = getattr(libc, "inotify_add_watch", None)
        if init is None or add_watch is None:
            return self
        init.argtypes = [ctypes.c_int]
        init.restype = ctypes.c_int
        add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
        add_watch.restype = ctypes.c_int
        fd = init(os.O_NONBLOCK | os.O_CLOEXEC)
        if fd < 0:
            return self
        try:
            watched = set()
            for path in (*self.paths.values(), *self.watch_paths):
                if path is None:
                    continue
                directory = os.path.abspath(os.fspath(Path(path).parent))
                if directory in watched:
                    continue
                watched.add(directory)
                add_watch(fd, os.fsencode(directory), _WATCH_MASK)
            # If no directories can be watched, rely on the fallback timer.
            self._loop.add_reader(fd, self._notify)
        except (OSError, NotImplementedError):
            os.close(fd)
        else:
            self._fd = fd
        return self

    async def __aexit__(self, *_):
        if self._fd is not None:
            self._loop.remove_reader(self._fd)
            os.close(self._fd)
            self._fd = None
        if self._owns_sources:
            close_output_sources(self._sources)

    def _notify(self):
        try:
            while True:
                if not os.read(self._fd, 65536):
                    break
        except BlockingIOError:
            pass
        self._changed.set()

    def snapshot_sizes(self):
        """Freeze bytes already present when job completion is observed."""
        return output_sizes(self.paths, self._sources)

    def read_available(self, paths=None, *, max_sizes=None):
        """Yield bounded chunks, committing their cursors as they are consumed."""
        return read_output_chunks(
            self.paths if paths is None else paths, self.offsets, self._sources,
            max_sizes=max_sizes, chunk_size=self.chunk_size,
        )

    async def wait_for_change(self, *, wakeup=None, stop=None):
        """Wake on file changes, completion, unsubscribe, or a filesystem timeout."""
        file_change = asyncio.create_task(self._changed.wait())
        try:
            await asyncio.wait(
                [file_change, *(future for future in (wakeup, stop) if future is not None)],
                timeout=self.poll_interval,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            file_change.cancel()
            await asyncio.gather(file_change, return_exceptions=True)
        self._changed.clear()


@portable
def open_output_sources(paths, sources):
    """Lazily open each logical stream once, retaining its original inode."""
    import os

    opened = {}
    seen = set()
    for kind, path in paths.items():
        if path is None:
            continue
        path = os.fspath(path)
        if path in seen:
            continue
        seen.add(path)
        if kind not in sources:
            try:
                sources[kind] = open(path, "rb", buffering=0)
            except FileNotFoundError:
                continue
        opened[kind] = sources[kind]
    return opened


@portable
def close_output_sources(sources):
    for source in sources.values():
        source.close()
    sources.clear()


@portable
@depends_on(open_output_sources)
def output_sizes(paths, sources):
    import os

    opened = open_output_sources(paths, sources)
    return {kind: os.fstat(opened[kind].fileno()).st_size if kind in opened else 0
            for kind in paths}


@portable
@depends_on(open_output_sources)
def read_output_chunks(paths, offsets, sources, *, max_sizes=None, chunk_size=65536):
    """Read bounded chunks from the original files, never a replacement path.

    A None cursor starts at EOF; numeric cursors cannot silently rewind.
    """
    import os

    for kind, source in open_output_sources(paths, sources).items():
        size = os.fstat(source.fileno()).st_size
        offset = offsets[kind]
        if offset is None:
            offsets[kind] = size
            continue
        if size < offset:
            raise ValueError(f"Offset is beyond the spool: {paths[kind]}")
        remaining = chunk_size if max_sizes is None else min(chunk_size, max(0, max_sizes[kind] - offset))
        source.seek(offset)
        data = source.read(remaining)
        if data:
            # Commit only after the caller has successfully delivered this
            # chunk. A failed second channel must not consume its bytes.
            yield kind, data
        offsets[kind] = offset + len(data)


@portable
async def follow_file_output(read, sizes, offsets, emit, terminal, wait, *, enabled, active):
    """Wait for native completion and optionally drain a frozen output range."""
    import asyncio
    import inspect

    result = None
    limits = None
    while active():
        copied = False
        for kind, data in read(limits) if enabled() else ():
            if not enabled():
                break
            pending = emit(kind, data)
            if inspect.isawaitable(pending):
                await pending
            copied = True
        if result is None:
            result = terminal()
            if inspect.isawaitable(result):
                result = await result
            if result is not None:
                if not enabled():
                    return result, False
                limits = sizes()
                # Bytes can appear after the preceding read, but before the
                # terminal observation. Attempt the frozen range immediately.
                if not copied and any(size and (offsets[kind] is None or offsets[kind] < size)
                                      for kind, size in limits.items()):
                    continue
        if result is not None:
            if not enabled():
                return result, False
            if all(size == 0 or offsets[kind] is not None and offsets[kind] >= size
                   for kind, size in limits.items()):
                return result, True
            if not copied:
                # The producer is done: an unreadable or removed remainder
                # cannot be delivered, and must not keep the observer alive.
                return result, False
        if copied:
            await asyncio.sleep(0)
        else:
            await wait()
    return result, False


async def follow_growing_files(paths, on_chunk, terminal, *, offsets=None, poll_interval=1.0,
                               wakeup=None, stop=None, sources=None):
    """Observe growing files with filesystem notifications when available.

    Return native facts and whether the requested output was drained.
    Stop cooperatively between complete read batches.
    """
    async with GrowingFileTail(paths, offsets=offsets, poll_interval=poll_interval,
                               sources=sources) as tail:
        async def wait():
            await tail.wait_for_change(
                wakeup=wakeup if wakeup is None or not wakeup.done() else None,
                stop=stop,
            )

        def active():
            return stop is None or not stop.done()

        return await follow_file_output(
            lambda limits: tail.read_available(max_sizes=limits), tail.snapshot_sizes, tail.offsets,
            on_chunk, terminal, wait, enabled=active, active=active,
        )


@portable
@depends_on(read_output_chunks, output_sizes, follow_file_output)
async def follow_polled_files(paths, on_chunk, terminal, *, offsets, poll_interval, wakeup, stop, sources):
    """Portable reader using timed filesystem checks instead of inotify."""
    import asyncio

    async def wait():
        pending = [future for future in (wakeup, stop) if not future.done()]
        if pending:
            await asyncio.wait(pending, timeout=poll_interval, return_when=asyncio.FIRST_COMPLETED)

    def active():
        return not stop.done()

    return await follow_file_output(
        lambda limits: read_output_chunks(paths, offsets, sources, max_sizes=limits),
        lambda: output_sizes(paths, sources), offsets, on_chunk, terminal, wait,
        enabled=active, active=active,
    )


@portable
@depends_on(follow_polled_files, close_output_sources)
async def observe_file_subscription(completion, paths, sinks, offsets, control, *,
                                    output, poll_interval, reader=None, cancel=None,
                                    disconnect_on_eof=True):
    """Wait for native completion independently of an optional file reader.

    The reader is created only for an active subscription. It is stopped
    cooperatively, never in the middle of an output frame. A failed reader
    stays stopped until another explicit follow request. Already-open file
    descriptors are retained across pauses; no reading or watching takes
    place while unsubscribed. The completion
    coroutine owns the backend's native wait; this operation owns its task.
    A Direct execution can also provide its native cancellation callback.
    EOF normally ends an independent observation; a detached execution
    may instead keep waiting for its own native completion.
    """
    import asyncio
    import inspect

    if reader is None:
        reader = follow_polled_files

    waiting = asyncio.create_task(completion)
    controls = tail = stop = None
    drained = False
    output_error = None
    sources = {}

    def terminal():
        return waiting.result() if waiting.done() else None

    async def emit(kind, data):
        pending = sinks[kind].write(data)
        if inspect.isawaitable(pending):
            await pending
        sinks[kind].flush()

    def start():
        nonlocal tail, stop, drained
        if not any(paths.values()):
            drained = True
            return
        stop = asyncio.get_running_loop().create_future()
        drained = False
        tail = asyncio.create_task(reader(
            paths, emit, terminal, offsets=offsets, poll_interval=poll_interval,
            wakeup=waiting, stop=stop, sources=sources,
        ))

    try:
        if output:
            start()
        if control is not None:
            controls = asyncio.create_task(control.readline())
        while not waiting.done() or tail is not None:
            tasks = [task for task in (waiting, controls, tail)
                     if task is not None and (task is not waiting or not task.done())]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            if waiting in done:
                waiting.result()  # A failed native wait is not a reader error.
            if tail in done:
                try:
                    _, drained = tail.result()
                    drained = output and drained
                except Exception as exc:
                    output_error = str(exc)
                    drained = False
                tail = stop = None
            if controls in done:
                line = controls.result()
                controls = None
                if not line:
                    if disconnect_on_eof and not (waiting.done() and tail is None):
                        raise ConnectionError("File observation disconnected")
                    continue
                command = line.strip()
                controls = asyncio.create_task(control.readline())
                if command == b"cancel" and cancel is not None:
                    if not waiting.done():
                        cancel()
                elif command == b"follow":
                    output = True
                    if tail is None and not drained:
                        start()
                elif command == b"unfollow":
                    output = False
                    drained = False
                    if tail is not None:
                        stop.set_result(None)
                        # Complete the current write before servicing the next
                        # command. The next iteration retires the stopped reader.
                        await asyncio.wait((tail,))
                else:
                    raise ValueError("Unknown observation control command")
        facts = await waiting
        return facts, output and drained, output_error
    finally:
        tasks = [task for task in (waiting, controls, tail) if task is not None]
        for task in tasks:
            task.cancel()
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            close_output_sources(sources)


@portable
@depends_on(observe_file_subscription)
async def controlled_named_files(paths, sinks, offsets, terminal, poll_interval, *, output, control):
    """Observe files on one controlled connection; no backend ownership."""
    import asyncio
    import inspect

    async def completion():
        while True:
            result = terminal()
            if inspect.isawaitable(result):
                result = await result
            if result is not None:
                return result
            await asyncio.sleep(poll_interval)

    facts, drained, error = await observe_file_subscription(
        completion(), paths, sinks, offsets, control, output=output, poll_interval=poll_interval,
    )
    if control is not None:
        facts = dict(facts, output_drained=drained)
    if error is not None:
        facts = dict(facts, output_error=error)
    return facts


@portable
@depends_on(controlled_named_files)
async def replay_named_output(facts, stdout, stderr, *, stdout_path, stderr_path,
                              stdout_offset, stderr_offset, output=True, control=None):
    """Replay saved bytes after a subscription races with native completion.

    This operation reads only the named streams; it never queries a backend.
    Return the caller's already known terminal facts unchanged.
    """
    return await controlled_named_files(
        {"out": stdout_path, "err": stderr_path},
        {"out": stdout, "err": stderr},
        {"out": stdout_offset, "err": stderr_offset},
        lambda: facts, 0.2, output=output, control=control,
    )
