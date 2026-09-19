import os
import subprocess
import sys

import pytest

from temporal_engine.lock import SchedulerLock, SchedulerLockHeld, _pid_alive


def _dead_pid() -> int:
    """A PID that verifiably belonged to a real process that has exited."""
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    return proc.pid


def test_pid_alive_is_true_for_this_process_and_false_for_an_exited_one():
    assert _pid_alive(os.getpid()) is True
    assert _pid_alive(_dead_pid()) is False


def test_stale_lock_from_a_dead_process_is_reclaimed(tmp_path):
    lock_path = tmp_path / "scheduler.lock"
    lock_path.write_text(str(_dead_pid()))  # simulates a force-killed holder

    lock = SchedulerLock(lock_path)
    lock.acquire()  # must not raise -- the holder is gone
    assert lock_path.read_text() == str(os.getpid())
    lock.release()


def test_lock_held_by_a_live_process_is_not_stolen(tmp_path):
    lock_path = tmp_path / "scheduler.lock"
    lock_path.write_text(str(os.getpid()))  # this test process is very much alive

    with pytest.raises(SchedulerLockHeld):
        SchedulerLock(lock_path).acquire()
    assert lock_path.exists()  # and the lock file was left untouched


def test_corrupt_empty_lock_file_is_treated_as_stale(tmp_path):
    lock_path = tmp_path / "scheduler.lock"
    lock_path.write_text("")  # holder died between create and write

    lock = SchedulerLock(lock_path)
    lock.acquire()
    lock.release()


def test_acquire_creates_the_lock_file_and_release_removes_it(tmp_path):
    lock_path = tmp_path / "scheduler.lock"
    lock = SchedulerLock(lock_path)

    lock.acquire()
    assert lock_path.exists()

    lock.release()
    assert not lock_path.exists()


def test_second_acquire_fails_while_first_is_held(tmp_path):
    lock_path = tmp_path / "scheduler.lock"
    first = SchedulerLock(lock_path)
    second = SchedulerLock(lock_path)

    first.acquire()
    try:
        with pytest.raises(SchedulerLockHeld):
            second.acquire()
    finally:
        first.release()


def test_lock_is_acquirable_again_after_release(tmp_path):
    lock_path = tmp_path / "scheduler.lock"
    first = SchedulerLock(lock_path)
    first.acquire()
    first.release()

    second = SchedulerLock(lock_path)
    second.acquire()  # must not raise -- the path is free again
    second.release()


def test_usable_as_a_context_manager(tmp_path):
    lock_path = tmp_path / "scheduler.lock"

    with SchedulerLock(lock_path):
        assert lock_path.exists()

    assert not lock_path.exists()


def test_context_manager_releases_even_if_the_body_raises(tmp_path):
    lock_path = tmp_path / "scheduler.lock"

    with pytest.raises(ValueError):
        with SchedulerLock(lock_path):
            raise ValueError("something went wrong while holding the lock")

    assert not lock_path.exists()
