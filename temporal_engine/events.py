"""
TemporalEvent: the append-only unit of truth, per SPEC.md section 2.
Nothing is ever deleted or edited here -- task state is always *derived*
by folding these forward (see storage.replay), never written directly.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class EventType(str, Enum):
    TASK_CREATED = "TASK_CREATED"
    TASK_STARTED = "TASK_STARTED"
    TASK_WINDOW_ENDED = "TASK_WINDOW_ENDED"
    TASK_DEADLINE_BREACHED = "TASK_DEADLINE_BREACHED"
    TASK_COMPLETED = "TASK_COMPLETED"
    NEW_DAY = "NEW_DAY"
    ACTION_PROPOSED = "ACTION_PROPOSED"


SCHEMA_VERSION = 1


@dataclass
class TemporalEvent:
    event_type: EventType
    occurred_at: datetime   # valid time -- when it happened in the world
    recorded_at: datetime   # transaction time -- when the engine noticed
    task_id: Optional[str] = None
    payload: dict[str, Any] = field(default_factory=dict)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    schema_version: int = SCHEMA_VERSION
