"""Portable observations of a direct process and its original process group."""


from kkrunner.support.decorators import portable


@portable
def observe_process(remote_id):
    """Return (state, explanation), including descendants of an ended leader.

    A Linux group is identified by its leader's PID/start ticks and boot UUID.
    Unknown observations never authorize signalling. No KKRunner installation
    or third-party module is required on the target.
    """
    import os
    import subprocess
    import sys
    from uuid import UUID

    parts = remote_id.split(":") if isinstance(remote_id, str) else ()
    if len(parts) != 4 or parts[0] not in ("direct", "ps"):
        return "unknown", "A verifiable direct process identity is unavailable"
    try:
        pid = int(parts[1])
        if pid <= 0:
            raise ValueError("Invalid PID")
    except (ValueError, TypeError):
        return "unknown", "Invalid direct process identity"

    if parts[0] == "direct":
        if not sys.platform.startswith("linux"):
            return "unknown", "Linux process identity cannot be verified on this host"
        try:
            start = int(parts[2])
            boot = str(UUID(parts[3]))
            if start < 0 or boot != parts[3]:
                raise ValueError("Invalid process identity")
        except (TypeError, ValueError, OverflowError):
            return "unknown", "Invalid direct process identity"
        try:
            with open("/proc/sys/kernel/random/boot_id", encoding="ascii") as source:
                current_boot = source.read().strip()
            if current_boot != boot:
                return "finished", "Process belongs to an earlier boot; exit code unavailable"
            try:
                with open(f"/proc/{pid}/stat", encoding="ascii") as source:
                    fields = source.read().rsplit(")", 1)[1].split()
                if int(fields[19]) != start:
                    return "finished", "Original process identity ended; exit code unavailable"
                if int(fields[2]) != pid or int(fields[3]) != pid:
                    return "unknown", "Original process group cannot be verified"
                if fields[0] not in ("Z", "X", "x"):
                    return "running", None
            except FileNotFoundError:
                pass  # Its children can retain the original group and session.

            with os.scandir("/proc") as entries:
                for entry in entries:
                    if not entry.name.isdecimal():
                        continue
                    try:
                        with open(entry.path + "/stat", encoding="ascii") as source:
                            fields = source.read().rsplit(")", 1)[1].split()
                    except FileNotFoundError:
                        continue
                    if int(fields[2]) != pid or fields[0] in ("Z", "X", "x"):
                        continue
                    if int(fields[3]) != pid or int(fields[19]) < start:
                        return "unknown", "Original process group cannot be verified"
                    return "running", None
        except (OSError, ValueError, IndexError) as exc:
            return "unknown", f"Cannot verify process identity or group: {exc}"
    else:
        if sys.platform.startswith("linux"):
            return "unknown", "Non-Linux process identity cannot be verified on this host"
        if parts[3] != str(pid):
            return "unknown", "Invalid process group identity"
        try:
            probe = subprocess.run(
                ("ps", "-p", str(pid), "-o", "lstart=", "-o", "stat="),
                capture_output=True, text=True, check=False,
                env={**os.environ, "LC_ALL": "C"},
            )
            if probe.returncode not in (0, 1):
                return "unknown", probe.stderr.strip() or "Process query failed"
            fields = probe.stdout.split()
            if fields and "_".join(fields[:-1]).replace(":", "-") != parts[2]:
                return "finished", "Original process identity ended; exit code unavailable"
            if fields and fields[-1][0] not in ("Z", "X"):
                return "running", None
            probe = subprocess.run(
                ("ps", "-e", "-o", "pgid=,stat="),
                capture_output=True, text=True, check=True,
                env={**os.environ, "LC_ALL": "C"},
            )
            for line in probe.stdout.splitlines():
                fields = line.split()
                if (len(fields) >= 2 and fields[0].isdigit()
                        and int(fields[0]) == pid and fields[1][0] not in ("Z", "X")):
                    return "running", None
        except (OSError, subprocess.CalledProcessError):
            return "unknown", "Cannot verify process-group members"
    return "finished", "Process ended; exit code unavailable"
