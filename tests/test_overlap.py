from datetime import datetime, timedelta, timezone

from temporal_engine.overlap import find_conflicts, find_overlapping, windows_overlap
from temporal_engine.task import Task, TaskStatus

UTC = timezone.utc
T = datetime(2026, 9, 12, 9, 0, tzinfo=UTC)


def task(title, start_h, end_h):
    return Task.new(title, "UTC", scheduled_start=T + timedelta(hours=start_h), scheduled_end=T + timedelta(hours=end_h))


def test_back_to_back_windows_do_not_overlap():
    assert not windows_overlap(T, T + timedelta(hours=1), T + timedelta(hours=1), T + timedelta(hours=2))


def test_partial_and_contained_windows_overlap():
    assert windows_overlap(T, T + timedelta(hours=2), T + timedelta(hours=1), T + timedelta(hours=3))
    assert windows_overlap(T, T + timedelta(hours=4), T + timedelta(hours=1), T + timedelta(hours=2))


def test_find_overlapping_ignores_finished_tasks_and_the_excluded_task():
    a, b, c = task("a", 0, 2), task("b", 1, 3), task("c", 1, 3)
    b.status = TaskStatus.COMPLETED
    found = find_overlapping([a, b, c], T + timedelta(hours=1), T + timedelta(hours=2), exclude_id=c.id)
    assert [t.title for t in found] == ["a"]


def test_tasks_without_a_full_window_never_overlap_anything():
    only_start = Task.new("s", "UTC", scheduled_start=T)
    deadline_only = Task.new("d", "UTC", deadline=T + timedelta(hours=3))
    assert find_overlapping([only_start, deadline_only], T, T + timedelta(hours=5)) == []
    assert find_conflicts([only_start, deadline_only, task("w", 0, 5)]) == []


def test_find_conflicts_reports_each_overlapping_pair_once():
    a, b, c, d = task("a", 0, 3), task("b", 1, 2), task("c", 2, 4), task("d", 5, 6)
    pairs = {(x.title, y.title) for x, y in find_conflicts([d, c, b, a])}
    assert pairs == {("a", "b"), ("a", "c")}      # b/c only touch at 2; d is clear of everything
