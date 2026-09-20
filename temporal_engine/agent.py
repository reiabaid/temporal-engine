"""
Agent: the loop from the original design -- scheduler event -> agent
invocation -> LLM reasoning -> action -- that lets the engine act when
something happens instead of only when someone talks to the model.

OFF BY DEFAULT. Nothing here runs unless the host enables it, because it
makes paid API calls unattended. The properties that make that tolerable:

* Nothing reaches the model unless there is something to decide. Trigger
  events for tasks that have since finished are filtered out
  *deterministically* (a task the user completed after its window ended
  needs no reasoning), and a batch with nothing left never calls the model.
* At-most-once decisions, durably. A checkpoint event in the log records how
  far triggers have been handled, so a restart neither re-decides history
  nor re-pays for it; enabling the agent on an existing log skips the past.
  Idempotency keys are derived from the triggering events (not from the
  vendor's tool-call ids), so a crash after applying but before
  checkpointing and then re-deciding the same batch cannot apply anything a
  second time. This closes the "idempotency covers re-applying, not
  re-deciding" gap noted in SPEC.md.
* Bounded. At most `max_calls_per_batch` actions per batch, at most one
  batch per `min_interval_seconds`, exponential backoff on provider errors
  (without advancing the checkpoint, so nothing is lost), and every action
  still passes the deterministic validator, the move rate limit, and the
  confirmation hold for anything destructive.
* Exactly one process runs it: the scheduler holder. Others do nothing.
* The provider is blocking (SDK calls take seconds) so it runs on a worker
  thread -- but on a private deep copy of the data. Engine state itself is
  still touched only by the event-loop thread.
"""
from __future__ import annotations

import asyncio
import copy
import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Mapping, Optional

from temporal_engine.events import EventType, TemporalEvent
from temporal_engine.providers import LLMProvider, StubHeuristicLLM, TemporalContext
from temporal_engine.runtime import Runtime
from temporal_engine.storage import View
from temporal_engine.task import Task

log = logging.getLogger("temporal_engine")

# The events that are worth waking a model for. Deliberately small: a task
# starting, or the user creating/completing something, needs no reasoning.
TRIGGER_TYPES = [
    EventType.TASK_WINDOW_ENDED,
    EventType.TASK_DEADLINE_BREACHED,
    EventType.NEW_DAY,
]


@dataclass
class AgentConfig:
    min_interval_seconds: float = 30.0
    poll_seconds: float = 5.0
    max_calls_per_batch: int = 10
    max_context_tasks: int = 100     # bounds the prompt (and the bill) however many tasks exist
    max_creations_per_day: int = 30  # tasks the agent may create in 24h: a runaway model must not fill the log
    batch_limit: int = 50
    backoff_base_seconds: float = 5.0
    backoff_max_seconds: float = 300.0


class Agent:
    def __init__(self, runtime: Runtime, provider: LLMProvider, config: Optional[AgentConfig] = None):
        self.runtime = runtime
        self.provider = provider
        self.config = config or AgentConfig()
        self.last_error: Optional[str] = None
        self.last_decision_at: Optional[datetime] = None
        self._failures = 0
        self._next_allowed_at: Optional[datetime] = None

    # -------------------------------------------------------------- one step

    async def step(self) -> str:
        """Handle at most one batch. Returns what happened:
        not_scheduler | waiting | idle | error | decided."""
        rt = self.runtime
        if not rt.health.is_scheduler:
            return "not_scheduler"

        now = rt.clock.now()
        if self._next_allowed_at is not None and now < self._next_allowed_at:
            return "waiting"

        view = rt.store.refresh()
        cursor = self._cursor(view)

        events, _ = rt.store.events(
            since_seq=cursor, event_types=TRIGGER_TYPES, limit=self.config.batch_limit,
        )
        if not events:
            return "idle"
        through = events[-1].seq

        relevant = [e for e in events if self._is_relevant(view, e)]
        if not relevant:
            self._checkpoint(through, "-", 0, [], note="nothing relevant")
            return "idle"

        batch_id = hashlib.sha1("|".join(e.id for e in relevant).encode()).hexdigest()[:16]
        ctx = self._context(view, relevant, now)

        try:
            calls = await asyncio.to_thread(self.provider.decide, ctx)
        except Exception as exc:
            self._fail(exc, now)
            return "error"
        self._failures = 0
        self.last_error = None

        outcomes = []
        creations_left = self._creation_budget(now)
        for index, call in enumerate(calls[: self.config.max_calls_per_batch]):
            # Derived from the triggering events + slot, so re-deciding this
            # batch after a crash lands on the same keys.
            call.idempotency_key = f"agent:{batch_id}:{index}"
            call.caused_by = tuple(e.id for e in relevant)
            if call.action == "create_task":
                if not call.args.get("display_timezone"):
                    call.args["display_timezone"] = rt.day_boundary_tz
                if creations_left <= 0:
                    outcomes.append(rt.reject(call, "the agent's daily task-creation limit was reached")[0].payload["outcome"])
                    continue
                creations_left -= 1
            outcomes.append(rt.execute(call)[0].payload["outcome"])

        note = f"truncated {len(calls)} to {self.config.max_calls_per_batch}" if len(calls) > self.config.max_calls_per_batch else ""
        self._checkpoint(through, batch_id, len(outcomes), outcomes, note=note)
        self.last_decision_at = now
        self._next_allowed_at = now + timedelta(seconds=self.config.min_interval_seconds)
        return "decided"

    async def run_forever(self) -> None:
        while True:
            try:
                await self.step()
            except Exception as exc:  # a DB error etc.; nothing was checkpointed, so it is retried
                self._fail(exc, self.runtime.clock.now())
                log.exception("agent step failed")
            try:
                await asyncio.wait_for(self.runtime.new_events.wait(), timeout=self.config.poll_seconds)
            except asyncio.TimeoutError:
                pass
            self.runtime.new_events.clear()

    def status(self) -> dict:
        view = self.runtime.store.view
        return {
            "enabled": True,
            "provider": type(self.provider).__name__,
            "cursor": view.agent_cursor,
            "last_decision_at": self.last_decision_at.isoformat() if self.last_decision_at else None,
            "last_error": self.last_error,
            "consecutive_errors": self._failures,
        }

    # --------------------------------------------------------------- helpers

    def _cursor(self, view: View) -> int:
        """Where handled triggers end. First run on an existing log starts
        *now*: enabling the agent must not act on everything that ever
        happened."""
        if view.agent_cursor is None:
            self._checkpoint(view.last_seq, "-", 0, [], note="initial: history skipped")
            return self.runtime.store.view.agent_cursor
        return view.agent_cursor

    @staticmethod
    def _is_relevant(view: View, event: TemporalEvent) -> bool:
        if event.event_type == EventType.NEW_DAY:
            return any(not t.is_terminal() for t in view.tasks.values())
        task = view.tasks.get(event.task_id)
        return task is not None and not task.is_terminal()

    def _context(self, view: View, relevant: list[TemporalEvent], now: datetime) -> TemporalContext:
        """A private deep copy: the provider runs on a worker thread, and
        must not be able to observe or mutate live engine state."""
        unfinished: list[Task] = [t for t in view.tasks.values() if not t.is_terminal()]
        wanted = {e.task_id for e in relevant}

        def nearness(t: Task) -> float:
            return abs(((t.scheduled_start or t.deadline or now) - now).total_seconds())

        # The tasks the events are about always go in; the rest nearest-to-now first, up to the cap.
        chosen = [t for t in unfinished if t.id in wanted]
        others = sorted((t for t in unfinished if t.id not in wanted), key=nearness)
        chosen += others[: max(0, self.config.max_context_tasks - len(chosen))]
        return TemporalContext(
            now=now,
            events=copy.deepcopy(relevant),
            tasks=copy.deepcopy(chosen),
            timezone=self.runtime.day_boundary_tz,
            omitted_tasks=len(unfinished) - len(chosen),
        )

    def _checkpoint(self, through_seq: int, batch_id: str, proposed: int, outcomes: list, note: str = "") -> None:
        rt = self.runtime
        now = rt.clock.now()
        with rt.store.transaction():
            rt.store.append([TemporalEvent(
                EventType.AGENT_CHECKPOINT, now, now,
                payload={"through_seq": through_seq, "batch_id": batch_id,
                         "proposed": proposed, "outcomes": outcomes, "note": note},
            )])

    def _creation_budget(self, now: datetime) -> int:
        """How many more tasks the agent may create in the last-24h window, counted from the log so the
        limit survives restarts."""
        events, _ = self.runtime.store.events(
            since_time=now - timedelta(hours=24), event_types=[EventType.ACTION_PROPOSED], limit=1000,
        )
        used = sum(
            1 for e in events
            if e.payload.get("outcome") == "applied"
            and e.payload["action_call"]["idempotency_key"].startswith("agent:")
            and e.payload["action_call"]["action"] == "create_task"
        )
        return max(0, self.config.max_creations_per_day - used)

    def _fail(self, exc: BaseException, now: datetime) -> None:
        self._failures += 1
        self.last_error = f"{type(exc).__name__}: {exc}"
        delay = min(self.config.backoff_max_seconds, self.config.backoff_base_seconds * 2.0 ** self._failures)
        self._next_allowed_at = now + timedelta(seconds=delay)
        log.warning("agent provider failed (%d in a row), retrying in %.0fs: %s",
                    self._failures, delay, self.last_error)


def provider_from_env(env: Mapping[str, str]) -> Optional[LLMProvider]:
    """Build the provider named by TEMPORAL_ENGINE_PROVIDER, or None (agent
    off). SDKs are imported only for the provider actually chosen."""
    name = env.get("TEMPORAL_ENGINE_PROVIDER", "").strip().lower()
    if not name:
        return None
    model = env.get("TEMPORAL_ENGINE_MODEL") or None
    if name == "stub":
        return StubHeuristicLLM()
    if name == "anthropic":
        from temporal_engine.anthropic_provider import AnthropicProvider
        return AnthropicProvider(model=model) if model else AnthropicProvider()
    if name == "openai":
        if not model:
            raise ValueError("TEMPORAL_ENGINE_MODEL is required for the openai provider (no default is guessed)")
        from temporal_engine.openai_provider import OpenAIProvider
        return OpenAIProvider(model=model, base_url=env.get("TEMPORAL_ENGINE_BASE_URL") or None)
    raise ValueError(f"unknown TEMPORAL_ENGINE_PROVIDER {name!r} (expected stub, anthropic or openai)")
