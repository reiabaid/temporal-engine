# State-QA: can a model derive temporal state from a history?

> **Status: the harness is built and tested. No real model has been scored.** Everything below describes what the benchmark can and cannot show. The one table in this document is for *deliberately flawed reference programs*, not for any language model.

## The question

This project's premise is that an LLM should not track time itself, because when it has to, it makes errors a deterministic engine does not. That premise is testable. State-QA gives a model the plain-text history of a user's task list (what was created, completed, moved, cancelled) and the current time, and asks for each task's **current status**. The answer is scored by exact match, per task. No judge model is involved.

It does **not** measure decision quality (what to *do* about an overdue task; see `temporal_engine/scenarios.py`), planning ability, or general reasoning.

## How a case is built

1. A scripted history (a list of timestamped steps) is generated.
2. The script is **run through the real engine** with a simulated clock. The statuses read back are the ground truth, so the expected answers are never typed in by hand.
3. The model sees the history rendered as text, the status definitions, and the current time, and no engine vocabulary (a test enforces this: no `TASK_WINDOW_ENDED`, no statuses inside the history).

The definitions are in the prompt, so a model is not asked to guess our conventions, only to apply them. A real prompt (the daylight-saving spring-forward case):

```
History:
2026-03-05T08:00-05:00  created recurring task "Standup": every day at 09:00 America/New_York for 30 minutes, starting 2026-03-05

Current time: 2026-03-08T13:15Z

Answer with a single JSON object ... Tasks: "Standup@2026-03-07", "Standup@2026-03-08", "Standup@2026-03-09"
```
Expected: `WINDOW_ENDED`, `ACTIVE`, `SCHEDULED`. (09:00 is EST = 14:00Z on the 7th, but EDT = 13:00Z on the 8th, so at 13:15Z the 8th's occurrence is running. A model that assumes a fixed offset says `SCHEDULED`.)

## Categories

| Category | What it targets |
|---|---|
| `window_states` | The basics: future, in progress, ended long ago, ended recently, across zones. |
| `deadlines` | A missed deadline outranks an ended or active window; deadline-only tasks. |
| `completions` | Done early, in-window, late, past a deadline inside the window; cancelled; ignored. |
| `moves` | A rescheduled or carried-forward task is asked about by its *newest* version. |
| `local_midnight` | The time is given in UTC only, minutes after **local** midnight, where the UTC and local dates disagree. |
| `dst_recurrence` | Recurring tasks in six cases built on real 2026 daylight-saving transitions in the US, UK and Australia, including a skipped and a repeated local hour. Fixed, hand-built, not random. |

## Why you can trust the ground truth

- **A second implementation must agree.** `derive_status` states the same rules as a pure function. It must agree with the engine on every generated case across several seeds; a mutation that removed the deadline-only rule from the engine failed this check.
- **The DST answers are derived by hand**, from the real transition dates, independently of the engine (`test_the_dst_cases_match_answers_derived_by_hand...`). They were checked against the engine after being written, and a mutation that resolved a skipped local time the wrong way failed them.
- **The benchmark discriminates.** Programs with one deliberate flaw each must fail exactly the categories the flaw predicts and pass the rest:

| solver (not a model) | window_states | deadlines | completions | moves | local_midnight | dst_recurrence | all |
|---|---|---|---|---|---|---|---|
| engine (reference) | 40/40 | 60/60 | 60/60 | 40/40 | 30/30 | 36/36 | 100% |
| ignores deadlines | 40/40 | 30/60 | 50/60 | 40/40 | 30/30 | 36/36 | 85% |
| forgets to convert offsets | 18/40 | 38/60 | 57/60 | 27/40 | 10/30 | 24/36 | 65% |
| reads clock digits, not instants | 40/40 | 60/60 | 60/60 | 40/40 | 30/30 | 34/36 | 99% |
| assumes a fixed UTC offset | 40/40 | 60/60 | 60/60 | 40/40 | 30/30 | 28/36 | 97% |

(Seeds 0 and 1, 5 cases per category; the DST category is fixed, so here it repeats across seeds. `scripts/live_state_qa.py` dedupes it.) Notably, reading clock digits looks nearly perfect. The realistic daylight-saving mistake is assuming the offset never changes, and *that* one needs a dedicated probe.

## Running it on a model

```bash
pip install anthropic            # or: pip install openai
python scripts/live_state_qa.py --solver anthropic --model <id> --seed 0 --seed 1 --seed 2 --per-category 8 --json replies.json
python scripts/live_state_qa.py --solver openai --model <id> [--base-url http://localhost:11434/v1]
python scripts/live_state_qa.py --show-prompt deadlines        # look at a prompt first
```

It costs money and is not deterministic, so it lives outside `tests/`. Report, at minimum: the model and version, the seeds, the per-category counts **with their confidence intervals**, the `no answer` count (unparseable or refused replies score as wrong and are counted separately), and keep `replies.json` so the scoring can be audited.

## Threats to validity, read these before quoting any number

- **The categories were chosen by the author to be hard.** They target failures predicted in advance. A high error rate here shows *where* a model can fail, not *how often* real usage hits those cases. Do not report the overall percentage as a general measure of temporal ability.
- **The engine arm is the reference by construction.** "The engine beats the model" is therefore true by definition. The only claim under test is that **the baseline errs where the engine does not**. Whether that matters in real use is a separate, unmeasured question.
- **Small samples.** A few dozen tasks give wide intervals (the table prints Wilson 95% intervals). Repeated runs of the same case are not independent samples. Compare models only with overlapping-interval caution.
- **The baseline is given the current time in the prompt.** That is generous to it: it saves a clock-tool call it could get wrong. This is not literally "LLM plus a clock tool".
- **One prompt format.** Results can shift with wording. The definitions are ours; a model may read them differently. The prompt is printable so this can be inspected.
- **Boundary equality is avoided on purpose.** Cases keep a margin, so "exactly at the deadline" is not tested.
- **Synthetic, and public.** Cases are generated, but the generator is in the repo, so a future model could be trained on it.
- **Decisions are not covered.** A model can get every status right and still decide badly, and the reverse.

## What a result would support

If a model scores well below 100% on `deadlines`, `local_midnight` or `dst_recurrence` with intervals clear of 100%, that supports the narrow claim that *that model, on that prompt*, mis-derives temporal state in those situations, which is the situation the engine removes. A score at or near 100% would be a genuine and informative negative result about the premise for that model.
