import asyncio
import inspect

import temporal_engine.mcp_server as server


def test_every_registered_tool_is_async_so_none_runs_on_a_worker_thread():
    """The SDK runs a *sync* tool on a worker thread. Engine state (the task
    dict, the SQLite connection, the wake event) is deliberately touched by
    one thread only -- the event loop the scheduler runs on -- so a sync tool
    would silently reintroduce a data race."""
    tools = asyncio.run(server.mcp.list_tools())
    assert len(tools) >= 10
    for tool in tools:
        fn = getattr(server, tool.name)
        assert inspect.iscoroutinefunction(fn), f"tool {tool.name!r} must be `async def`"


def test_every_mutating_tool_accepts_an_idempotency_key():
    tools = asyncio.run(server.mcp.list_tools())
    mutating = {"create_task", "start_task", "complete_task", "reschedule_task",
                "carry_forward_task", "cancel_task"}
    by_name = {t.name: t for t in tools}
    assert mutating <= set(by_name)
    for name in mutating:
        assert "idempotency_key" in by_name[name].input_schema["properties"], name
