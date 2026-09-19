"""
Manual runner: score a REAL model on the decision scenarios in
temporal_engine/scenarios.py. Lives outside tests/ on purpose -- it costs
money, needs a key (or a local server), and a live model's output is not
deterministic, so it must never run in CI. pytest is restricted to tests/
by pytest.ini.

Examples (from the project root):

  pip install anthropic
  set ANTHROPIC_API_KEY=...
  python scripts/live_scenarios.py --provider anthropic

  pip install openai
  set OPENAI_API_KEY=...
  python scripts/live_scenarios.py --provider openai --model <model-id>

  # a local model through Ollama's OpenAI-compatible endpoint
  # (tool-calling reliability varies a lot by local model):
  python scripts/live_scenarios.py --provider openai --model llama3.1 \
      --base-url http://localhost:11434/v1

  python scripts/live_scenarios.py --provider stub     # the deterministic reference

Each scenario is run --runs times (default 3) because a single sample of a
stochastic model says little; the per-scenario pass count is the result.
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from temporal_engine.providers import StubHeuristicLLM
from temporal_engine.scenarios import SCENARIOS, run_scenario


def make_provider(args):
    if args.provider == "stub":
        return StubHeuristicLLM()
    if args.provider == "anthropic":
        from temporal_engine.anthropic_provider import AnthropicProvider
        return AnthropicProvider(model=args.model) if args.model else AnthropicProvider()
    if args.provider == "openai":
        if not args.model:
            sys.exit("--model is required for --provider openai (no default is guessed on purpose)")
        from temporal_engine.openai_provider import OpenAIProvider
        return OpenAIProvider(model=args.model, base_url=args.base_url)
    sys.exit(f"unknown provider {args.provider!r}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", required=True, choices=["stub", "anthropic", "openai"])
    parser.add_argument("--model")
    parser.add_argument("--base-url")
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()

    provider = make_provider(args)
    total_passed = total = 0
    for scenario in SCENARIOS:
        results = [run_scenario(provider, scenario) for _ in range(args.runs)]
        passed = sum(r.passed for r in results)
        total_passed, total = total_passed + passed, total + len(results)
        print(f"{scenario.name:32s} {passed}/{len(results)}")
        for r in results:
            if not r.passed:
                print(f"    proposed={r.proposed}")
                for f in r.failures:
                    print(f"      - {f}")
    print(f"\n{total_passed}/{total} scenario runs passed")


if __name__ == "__main__":
    main()
