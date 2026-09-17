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

A process that fails to acquire this lock must still be able to read the
database and answer requests -- it just must not call tick() itself. That
"read-only request handler" role isn't implemented here; this file only
provides the primitive that role would check.
"""
from __future__ import annotations

import os
from pathlib import Path


class SchedulerLockHeld(RuntimeError):
    """Raised when another process already holds the scheduler lock."""


class SchedulerLock:
    def __init__(self, lock_path: str | Path):
        self._lock_path = Path(lock_path)
        self._fd: int | None = None

    def acquire(self) -> None:
        try:
            # O_CREAT|O_EXCL is what makes this atomic even across
            # processes: the OS guarantees only one caller can win the
            # race to create this exact file, so there is no window
            # where two processes both believe they hold the lock.
            self._fd = os.open(str(self._lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(self._fd, str(os.getpid()).encode())
        except FileExistsError as exc:
            raise SchedulerLockHeld(
                f"scheduler lock already held: {self._lock_path}"
            ) from exc

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
