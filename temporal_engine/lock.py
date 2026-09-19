"""
SchedulerLock: ensures at most one process runs the scheduler loop (i.e.
calls tick()) against a given database at a time. Per SPEC.md section 7,
the realistic deployment is two MCP client processes (e.g. Claude Desktop
and Claude Code) pointed at the same server config -- WAL mode lets both
read the database concurrently, but nothing stops both from independently
running their own scheduler and firing the same event twice unless
something like this exists.

Implemented as a plain OS-level lock file (SPEC.md explicitly allows this
or a SQLite BEGIN IMMEDIATE transaction -- a lock file is simpler to
reason about and test, and doesn't tie the lock's lifetime to a single
long-held database transaction).

Stale-lock recovery: the lock file records the holder's PID. If a process
is killed without running its cleanup (e.g. an MCP host force-terminating
its child on app close), the file is left behind. On the next acquire we
check whether that PID is still alive and, if not, reclaim the lock
rather than silently running as a non-scheduler forever. Known
limitation: if the OS has since reused that PID for an unrelated live
process, the lock will look held -- rare, and fails safe (no duplicate
scheduler), so it is accepted rather than engineered around.

A process that fails to acquire this lock must still be able to read the
database and answer requests -- it just must not call tick() itself.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path


class SchedulerLockHeld(RuntimeError):
    """Raised when another live process already holds the scheduler lock."""


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if sys.platform == "win32":
        # os.kill(pid, 0) must NOT be used here: on Windows any signal
        # other than CTRL_C/CTRL_BREAK terminates the target process.
        import ctypes

        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        STILL_ACTIVE = 259
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            return False
        try:
            exit_code = ctypes.c_ulong()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                return True  # can't tell; fail safe (treat as alive)
            return exit_code.value == STILL_ACTIVE
        finally:
            kernel32.CloseHandle(handle)

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, just not ours to signal
    return True


class SchedulerLock:
    def __init__(self, lock_path: str | Path):
        self._lock_path = Path(lock_path)
        self._fd: int | None = None

    def acquire(self) -> None:
        if self._try_create():
            return

        if self._holder_is_dead():
            self._lock_path.unlink(missing_ok=True)
            if self._try_create():
                return

        raise SchedulerLockHeld(f"scheduler lock already held: {self._lock_path}")

    def _try_create(self) -> bool:
        try:
            # O_CREAT|O_EXCL is what makes this atomic even across
            # processes: the OS guarantees only one caller can win the
            # race to create this exact file.
            self._fd = os.open(str(self._lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            return False
        os.write(self._fd, str(os.getpid()).encode())
        return True

    def _holder_is_dead(self) -> bool:
        try:
            pid = int(self._lock_path.read_text().strip())
        except (FileNotFoundError, ValueError):
            # Unreadable/empty lock file: holder crashed mid-write.
            # Missing file means someone just released it -- also fine
            # to retry.
            return True
        return not _pid_alive(pid)

    def release(self) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        self._lock_path.unlink(missing_ok=True)

    def __enter__(self) -> "SchedulerLock":
        self.acquire()
        return self

    def __exit__(self, *exc_info) -> None:
        self.release()
