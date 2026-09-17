import pytest

from temporal_engine.lock import SchedulerLock, SchedulerLockHeld


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
