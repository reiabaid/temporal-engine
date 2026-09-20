import json

import pytest

from temporal_engine.state_qa import (
    CATEGORIES, AssumesFixedOffset, EngineSolver, IgnoresDeadlines, LLMSolver, MixesTimezones, ReadsClockDigits,
    derive_status, evaluate, generate, parse_answer, render_prompt, wilson_interval,
)

CASES = generate(seed=0, per_category=4)


def score(solver, category=None):
    cases = [c for c in CASES if category is None or c.category == category]
    return evaluate(solver, cases)


# ---------------------------------------------------------------- integrity

def test_every_category_is_represented_and_every_case_has_ground_truth():
    assert {c.category for c in CASES} == set(CATEGORIES)
    for case in CASES:
        assert set(case.expected) == set(case.asked) and all(case.expected.values())


def test_generation_is_deterministic_for_a_seed_and_varies_between_seeds():
    again = generate(seed=0, per_category=4)
    assert [(c.id, c.expected, render_prompt(c)) for c in CASES] == [(c.id, c.expected, render_prompt(c)) for c in again]
    other = generate(seed=1, per_category=4)
    assert [render_prompt(c) for c in CASES] != [render_prompt(c) for c in other]


@pytest.mark.parametrize("seed", [0, 1, 2, 3])
def test_an_independent_statement_of_the_rules_agrees_with_the_engine_on_every_case(seed):
    """The ground truth comes from running the engine; derive_status is the
    same semantics written a second, separate way. Any disagreement means one
    of them (or the prompt's definitions) is wrong."""
    for case in generate(seed=seed, per_category=6):
        for key, fact in case.facts.items():
            assert derive_status(fact, case.now) == case.expected[key], (case.id, key)


def test_the_answers_are_not_all_the_same_so_guessing_one_status_scores_poorly():
    from collections import Counter
    counts = Counter(v for c in CASES for v in c.expected.values())
    assert len(counts) >= 6                                   # the statuses actually vary
    assert max(counts.values()) / sum(counts.values()) < 0.35


def test_the_engine_arm_scores_perfectly_when_re_run_from_the_history():
    report = score(EngineSolver())
    assert report.total().tasks_correct == report.total().tasks


def test_prompts_carry_the_history_and_definitions_but_never_the_answers_or_engine_vocabulary():
    for case in CASES:
        prompt = render_prompt(case)
        assert "Status definitions" in prompt and "Current time:" in prompt
        for key in case.asked:
            assert f'"{key}"' in prompt
        for internal in ("TASK_WINDOW_ENDED", "NEW_DAY", "TASK_CREATED", "ACTION_PROPOSED", "tick"):
            assert internal not in prompt                     # no engine-derived state leaks to the baseline
        history = prompt.split("History:")[1].split("Current time:")[0]
        assert "WINDOW_ENDED" not in history and "OVERDUE" not in history   # the history states facts, not statuses


def test_local_midnight_cases_give_the_time_in_utc_only():
    for case in CASES:
        if case.category == "local_midnight":
            assert render_prompt(case).split("Current time:")[1].split("\n")[0].strip().endswith("Z")


# ------------------------------------------- the benchmark can tell errors apart

def test_ignoring_deadlines_fails_the_deadline_category_and_not_plain_window_reasoning():
    assert score(IgnoresDeadlines(), "window_states").total().tasks_correct == score(IgnoresDeadlines(), "window_states").total().tasks
    deadlines = score(IgnoresDeadlines(), "deadlines").total()
    assert deadlines.tasks_correct < deadlines.tasks


def test_forgetting_to_convert_offsets_fails_wherever_the_zone_is_not_utc_and_is_caught_at_midnight():
    midnight = score(MixesTimezones(), "local_midnight").total()
    assert midnight.tasks_correct < midnight.tasks
    assert score(MixesTimezones()).total().tasks_correct < score(EngineSolver()).total().tasks_correct


def test_reading_clock_digits_is_fine_until_daylight_saving_and_fails_exactly_there():
    report = score(ReadsClockDigits())
    for category in CATEGORIES:
        cat = report.by_category[category]
        if category == "dst_recurrence":
            assert cat.tasks_correct < cat.tasks, "clock-digit reasoning should fail on DST cases"
        else:
            assert cat.tasks_correct == cat.tasks, f"clock-digit reasoning should be fine on {category}"


def test_a_solver_that_crashes_or_answers_nothing_scores_zero_without_aborting():
    class Crashes:
        name = "crashes"
        def solve(self, case): raise RuntimeError("boom")
    assert score(Crashes()).total().tasks_correct == 0
    assert score(LLMSolver(lambda prompt: "I cannot help with that")).total().tasks_correct == 0


# ------------------------------------------------------------------ parsing

def test_parse_answer_extracts_json_from_chatty_and_fenced_replies_and_normalises_case():
    asked = ["A", "B"]
    assert parse_answer('```json\n{"A": "active", "B": "Overdue"}\n```', asked) == {"A": "ACTIVE", "B": "OVERDUE"}
    assert parse_answer('Sure! Here you go: {"A": "SCHEDULED", "B": "CREATED"} Hope that helps.', asked) == {"A": "SCHEDULED", "B": "CREATED"}
    assert parse_answer('{"not json": ', asked) == {}
    assert parse_answer('{"A": "ACTIVE", "extra": "x"}', asked) == {"A": "ACTIVE"}       # unasked keys are ignored


def test_an_llm_solver_that_answers_perfectly_scores_perfectly():
    by_prompt = {render_prompt(c): json.dumps(c.expected) for c in CASES}
    perfect = LLMSolver(lambda prompt: "Answer: " + by_prompt[prompt])
    report = score(perfect)
    assert report.total().tasks_correct == report.total().tasks


# ------------------------------------------------------------------- statistics

def test_wilson_interval_is_sane():
    assert wilson_interval(0, 0) == (0.0, 1.0)
    lo, hi = wilson_interval(10, 10)
    assert hi == 1.0 and 0.7 < lo < 1.0                       # 10/10 is not "certainly 100%"
    lo, hi = wilson_interval(50, 100)
    assert lo < 0.5 < hi and hi - lo < 0.21
    narrow = wilson_interval(500, 1000)
    assert narrow[1] - narrow[0] < wilson_interval(50, 100)[1] - wilson_interval(50, 100)[0]


def test_the_report_table_names_every_category_and_a_total():
    table = score(EngineSolver()).table()
    for category in CATEGORIES:
        assert category in table
    assert "ALL" in table and "95% CI" in table


# ------------------------------- DST truths worked out by hand, not read from the engine

def test_the_dst_cases_match_answers_derived_by_hand_from_the_real_2026_transitions():
    """The engine produces the ground truth for every other case, so a DST
    mistake in the engine's own arithmetic would silently become 'correct'.
    These expectations were reasoned out independently:
      spring: 09:00 EST=14:00Z on the 7th (over by 13:15Z on the 8th), 09:00 EDT=13:00Z on the 8th (running at 13:15Z)
      fall:   01:30 happens twice on Nov 1; the first (EDT, 05:30Z) is meant, so 05:30-06:30Z runs at 06:15Z
      gap:    02:30 does not exist on Mar 8; it shifts forward to 03:30 EDT = 07:30Z, so 07:30-08:30Z runs at 07:45Z"""
    truth = {
        # 09:00 EDT = 13:00Z on Oct 31 (over); from Nov 1 the zone is EST so 09:00 = 14:00Z, running at 14:10Z
        "dst_recurrence-us-morning-after": {"Standup@2026-10-31": "WINDOW_ENDED", "Standup@2026-11-01": "ACTIVE", "Standup@2026-11-02": "SCHEDULED"},
        # 07:00 GMT = 07:00Z on Mar 28 (over); BST from Mar 29 so 07:00 = 06:00Z, running at 06:10Z
        "dst_recurrence-uk-spring": {"Standup@2026-03-28": "WINDOW_ENDED", "Standup@2026-03-29": "ACTIVE", "Standup@2026-03-30": "SCHEDULED"},
        # Sydney is +11 until Apr 5 03:00 then +10: 09:00 on the 4th = 22:00Z on the 3rd (over); on the 5th = 23:00Z on the 4th, running at 23:10Z
        "dst_recurrence-sydney-autumn": {"Standup@2026-04-04": "WINDOW_ENDED", "Standup@2026-04-05": "ACTIVE", "Standup@2026-04-06": "SCHEDULED"},
        "dst_recurrence-spring": {"Standup@2026-03-07": "WINDOW_ENDED", "Standup@2026-03-08": "ACTIVE", "Standup@2026-03-09": "SCHEDULED"},
        "dst_recurrence-fall": {"Standup@2026-10-31": "WINDOW_ENDED", "Standup@2026-11-01": "ACTIVE", "Standup@2026-11-02": "SCHEDULED"},
        "dst_recurrence-gap": {"Standup@2026-03-07": "WINDOW_ENDED", "Standup@2026-03-08": "ACTIVE", "Standup@2026-03-09": "SCHEDULED"},
    }
    cases = {c.id: c for c in CASES if c.category == "dst_recurrence"}
    assert set(cases) == set(truth)
    for case_id, expected in truth.items():
        assert cases[case_id].expected == expected, case_id


def test_assuming_a_fixed_utc_offset_fails_exactly_the_dst_cases_and_nothing_else():
    """The realistic daylight-saving mistake. It must be visibly wrong on DST
    cases in several regions -- not merely on one or two edge tasks -- and
    harmless everywhere else."""
    report = score(AssumesFixedOffset())
    for category in CATEGORIES:
        cat = report.by_category[category]
        if category == "dst_recurrence":
            assert cat.perfect_cases < cat.cases
        else:
            assert cat.tasks_correct == cat.tasks, category
    dst_cases = [c for c in CASES if c.category == "dst_recurrence"]
    wrong_cases = [c for c in dst_cases if AssumesFixedOffset().solve(c) != c.expected]
    assert len(wrong_cases) >= 3                                # several regions, not an edge quirk


# ------------------------------------------- historical and dependency categories

from collections import Counter

from temporal_engine.state_qa import AnswersForThePresent, IgnoresDependencies


def test_historical_cases_ask_about_the_past_and_the_history_runs_past_the_question():
    for case in (c for c in CASES if c.category == "historical"):
        assert case.present is not None and case.present > case.now
        assert any(s.at > case.now for s in case.steps)                       # later events exist, and must be ignored
        prompt = render_prompt(case)
        assert "Question time" in prompt and "EARLIER moment" in prompt
    counts = Counter(v for c in CASES if c.category == "historical" for v in c.expected.values())
    assert {"ACTIVE", "WINDOW_ENDED", "SCHEDULED", "COMPLETED"} <= set(counts)   # not one status repeated


def test_dependency_cases_cover_blocked_and_every_way_of_not_being_blocked():
    counts = Counter(v for c in CASES if c.category == "dependencies" for v in c.expected.values())
    assert counts["BLOCKED"] > 0 and counts["ACTIVE"] > 0 and counts["WINDOW_ENDED"] > 0
    for case in (c for c in CASES if c.category == "dependencies"):
        prompt = render_prompt(case)
        assert "depends on" in prompt and "BLOCKED" in prompt


def test_the_dependency_and_historical_notes_appear_only_where_they_apply():
    for case in CASES:
        prompt = render_prompt(case)
        assert ("BLOCKED" in prompt) == (case.category == "dependencies")
        assert ("EARLIER moment" in prompt) == (case.category == "historical")


def test_ignoring_dependencies_fails_exactly_the_dependency_category():
    report = score(IgnoresDependencies())
    for category in CATEGORIES:
        cat = report.by_category[category]
        if category == "dependencies":
            assert cat.tasks_correct < cat.tasks
        else:
            assert cat.tasks_correct == cat.tasks, category


def test_answering_for_the_present_fails_exactly_the_historical_category():
    report = score(AnswersForThePresent())
    for category in CATEGORIES:
        cat = report.by_category[category]
        if category == "historical":
            assert cat.tasks_correct < cat.tasks
        else:
            assert cat.tasks_correct == cat.tasks, category
