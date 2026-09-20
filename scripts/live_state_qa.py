"""
Score a solver on State-QA. Lives outside tests/ (pytest is restricted to
tests/) because the real-model arms cost money and are not deterministic.

  python scripts/live_state_qa.py --solver engine                 # reference: 100% by construction
  python scripts/live_state_qa.py --solver clock-digits           # a deliberately flawed reference solver
  python scripts/live_state_qa.py --solver anthropic --model <id>
  python scripts/live_state_qa.py --solver openai --model <id> [--base-url http://localhost:11434/v1]
  python scripts/live_state_qa.py --show-prompt window_states     # print one prompt and exit

Use several --seed values and --runs > 1 for a stochastic model, and read the
confidence intervals: with a few dozen tasks they are wide. Repeated runs of
the same case are NOT independent samples. --json writes every raw reply so
the scoring can be audited.
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from temporal_engine.state_qa import (
    CATEGORIES, AssumesFixedOffset, EngineSolver, IgnoresDeadlines, LLMSolver, MixesTimezones, ReadsClockDigits,
    evaluate, generate, render_prompt,
)


def build_solver(args):
    offline = {"engine": EngineSolver, "ignores-deadlines": IgnoresDeadlines,
               "clock-digits": ReadsClockDigits, "mixes-timezones": MixesTimezones,
               "fixed-offset": AssumesFixedOffset}
    if args.solver in offline:
        return offline[args.solver](), None
    if not args.model:
        sys.exit("--model is required for a real model (no default is guessed)")
    from temporal_engine import text_completion
    if args.solver == "anthropic":
        backend = text_completion.AnthropicText(args.model, temperature=args.temperature)
    else:
        backend = text_completion.OpenAIText(args.model, base_url=args.base_url, temperature=args.temperature)
    transcript = []

    def complete(prompt):
        reply = backend.complete(prompt)
        transcript.append({"prompt": prompt, "reply": reply})
        return reply

    return LLMSolver(complete, name=backend.name), transcript


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--solver", choices=["engine", "ignores-deadlines", "clock-digits", "mixes-timezones",
                                             "fixed-offset", "anthropic", "openai"])
    parser.add_argument("--model")
    parser.add_argument("--base-url")
    parser.add_argument("--temperature", type=float)
    parser.add_argument("--seed", type=int, action="append", help="repeatable; default 0")
    parser.add_argument("--per-category", type=int, default=5)
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--show-prompt", choices=CATEGORIES)
    parser.add_argument("--json", help="write raw replies here")
    args = parser.parse_args()

    seeds = args.seed or [0]
    cases, seen = [], set()
    for seed in seeds:
        for case in generate(seed=seed, per_category=args.per_category):
            if case.id not in seen:            # the DST category is fixed, so it repeats across seeds
                seen.add(case.id)
                cases.append(case)

    if args.show_prompt:
        case = next(c for c in cases if c.category == args.show_prompt)
        print(render_prompt(case))
        print("\n--- expected (from the engine) ---")
        print(json.dumps(case.expected, indent=2))
        return
    if not args.solver:
        parser.error("--solver is required")

    solver, transcript = build_solver(args)
    repeated = [c for _ in range(args.runs) for c in cases]
    report = evaluate(solver, repeated)
    print(f"{len(cases)} cases x {args.runs} run(s), seeds {seeds}\n")
    print(report.table())
    if args.json and transcript is not None:
        Path(args.json).write_text(json.dumps({"solver": solver.name, "seeds": seeds, "transcript": transcript}, indent=2))
        print(f"\nraw replies written to {args.json}")


if __name__ == "__main__":
    main()
