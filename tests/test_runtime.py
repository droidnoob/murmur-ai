"""Smoke tests for ``murmur.AgentRuntime``."""

from __future__ import annotations

import pytest
from tests.conftest import MockBackend

from murmur.agent import Agent
from murmur.core.errors import RegistryError, SpecValidationError
from murmur.runtime import AgentRuntime
from murmur.types import TaskSpec


async def test_run_returns_typed_result(echo_agent, mock_backend: MockBackend) -> None:
    runtime = AgentRuntime(backend=mock_backend)
    result = await runtime.run(echo_agent, TaskSpec(input="hi"))
    assert result.is_ok()
    assert result.agent_name == echo_agent.name


async def test_gather_runs_each_task(echo_agent, mock_backend: MockBackend) -> None:
    runtime = AgentRuntime(backend=mock_backend)
    tasks = [TaskSpec(input=f"q{i}") for i in range(5)]
    results = await runtime.gather(echo_agent, tasks=tasks, max_concurrency=2)
    assert len(results) == 5
    assert all(r.is_ok() for r in results)


async def test_unknown_agent_name_without_registry_raises(
    mock_backend: MockBackend,
) -> None:
    runtime = AgentRuntime(backend=mock_backend)
    with pytest.raises(RegistryError, match="no registry"):
        await runtime.run("missing", TaskSpec(input="x"))


def test_invalid_broker_scheme_rejected() -> None:
    with pytest.raises(SpecValidationError, match="unsupported broker URL"):
        AgentRuntime(broker="ftp://example.com")


def test_no_broker_picks_async_backend() -> None:
    runtime = AgentRuntime()
    assert runtime.backend.__class__.__name__ == "AsyncBackend"


def test_kafka_broker_picks_job_backend() -> None:
    runtime = AgentRuntime(broker="kafka://localhost:9092")
    assert runtime.backend.__class__.__name__ == "JobBackend"


@pytest.mark.parametrize("max_concurrency", [0, -1])
async def test_gather_rejects_invalid_concurrency(
    echo_agent,
    mock_backend: MockBackend,
    max_concurrency: int,
) -> None:
    runtime = AgentRuntime(backend=mock_backend)
    with pytest.raises(SpecValidationError, match="max_concurrency"):
        await runtime.gather(
            echo_agent,
            tasks=[TaskSpec(input="x")],
            max_concurrency=max_concurrency,
        )


# ---------------------------------------------------------------------------
# gather fail_fast
# ---------------------------------------------------------------------------


async def test_gather_fail_fast_false_returns_partial_failures(
    echo_agent,
) -> None:
    """Default behavior: per-task errors land in AgentResult.error."""
    from pydantic import BaseModel

    from murmur.core.errors import SpawnError
    from murmur.core.protocols.backend import BackendStatus
    from murmur.types import AgentHandle, AgentResult, ResultMetadata

    class _Out(BaseModel):
        text: str

    class FlakyBackend:
        """Fails task 1; succeeds on others. No `gather` method — runtime
        falls back to the semaphore path."""

        name = "flaky"

        async def spawn(self, agent, task, context):  # noqa: ARG002
            return AgentHandle(
                agent_name=agent.name, task_id=task.id, backend=self.name
            )

        async def status(self, handle):  # noqa: ARG002
            return BackendStatus.COMPLETED

        async def kill(self, handle):  # noqa: ARG002
            return None

        async def result(self, handle):
            if handle.task_id.endswith("1"):
                raise SpawnError("task 1 broke")
            return AgentResult[BaseModel](
                output=_Out(text="ok"),
                error=None,
                metadata=ResultMetadata(backend=self.name),
                agent_name=handle.agent_name,
                task_id=handle.task_id,
            )

    runtime = AgentRuntime(backend=FlakyBackend())
    tasks = [TaskSpec(input=f"q-{i}", id=f"t-{i}") for i in range(3)]
    results = await runtime.gather(echo_agent, tasks=tasks, fail_fast=False)
    assert len(results) == 3
    assert results[0].is_ok()
    assert results[1].error is not None  # the failure
    assert "broke" in str(results[1].error)
    assert results[2].is_ok()


async def test_gather_fail_fast_true_reraises_first_error(
    echo_agent,
) -> None:
    """fail_fast=True re-raises the first task's error after the batch settles."""
    from murmur.core.errors import SpawnError
    from murmur.core.protocols.backend import BackendStatus
    from murmur.types import AgentHandle, AgentResult, ResultMetadata

    class FailingBackend:
        name = "failing"

        async def spawn(self, agent, task, context):  # noqa: ARG002
            return AgentHandle(
                agent_name=agent.name, task_id=task.id, backend=self.name
            )

        async def status(self, handle):  # noqa: ARG002
            return BackendStatus.FAILED

        async def kill(self, handle):  # noqa: ARG002
            return None

        async def result(self, handle):
            if handle.task_id.endswith("0"):
                # First task fails — fail_fast surfaces THIS error.
                raise SpawnError("first one failed")
            return AgentResult(
                output=None,
                error=None,
                metadata=ResultMetadata(backend=self.name),
                agent_name=handle.agent_name,
                task_id=handle.task_id,
            )

    runtime = AgentRuntime(backend=FailingBackend())
    tasks = [TaskSpec(input=f"q-{i}", id=f"t-{i}") for i in range(3)]
    with pytest.raises(SpawnError, match="first one failed"):
        await runtime.gather(echo_agent, tasks=tasks, fail_fast=True)


# ---------------------------------------------------------------------------
# Pipeline middleware composition
# ---------------------------------------------------------------------------


async def test_run_timeout_middleware_translates_to_spawn_error(
    echo_agent,
) -> None:
    """``RuntimeOptions.timeout_seconds`` enforces a per-run cap."""
    from murmur.core.errors import SpawnError
    from murmur.core.protocols.backend import BackendStatus
    from murmur.runtime import RuntimeOptions
    from murmur.types import AgentHandle

    class SlowBackend:
        name = "slow"

        async def spawn(self, agent, task, context):  # noqa: ARG002
            return AgentHandle(
                agent_name=agent.name, task_id=task.id, backend=self.name
            )

        async def status(self, handle):  # noqa: ARG002
            return BackendStatus.RUNNING

        async def kill(self, handle):  # noqa: ARG002
            return None

        async def result(self, handle):  # noqa: ARG002
            import asyncio

            await asyncio.sleep(5)
            raise AssertionError("should have been cancelled by timeout")

    runtime = AgentRuntime(
        backend=SlowBackend(), options=RuntimeOptions(timeout_seconds=0.05)
    )
    with pytest.raises(SpawnError, match="timed out"):
        await runtime.run(echo_agent, TaskSpec(input="x"))


async def test_run_retry_middleware_recovers_transient_failure(
    echo_agent,
) -> None:
    """``retry_max_attempts > 1`` retries transient ``SpawnError``."""
    from pydantic import BaseModel

    from murmur.core.errors import SpawnError
    from murmur.core.protocols.backend import BackendStatus
    from murmur.runtime import RuntimeOptions
    from murmur.types import AgentHandle, AgentResult, ResultMetadata

    class _Out(BaseModel):
        text: str

    attempts = {"count": 0}

    class FlakyBackend:
        name = "flaky"

        async def spawn(self, agent, task, context):  # noqa: ARG002
            return AgentHandle(
                agent_name=agent.name, task_id=task.id, backend=self.name
            )

        async def status(self, handle):  # noqa: ARG002
            return BackendStatus.COMPLETED

        async def kill(self, handle):  # noqa: ARG002
            return None

        async def result(self, handle):
            attempts["count"] += 1
            if attempts["count"] < 2:
                raise SpawnError("transient")
            return AgentResult[BaseModel](
                output=_Out(text="ok"),
                error=None,
                metadata=ResultMetadata(backend=self.name),
                agent_name=handle.agent_name,
                task_id=handle.task_id,
            )

    runtime = AgentRuntime(
        backend=FlakyBackend(),
        options=RuntimeOptions(retry_max_attempts=3, retry_backoff_factor=0.001),
    )
    result = await runtime.run(echo_agent, TaskSpec(input="x"))
    assert result.is_ok()
    assert attempts["count"] == 2


async def test_run_default_options_are_applied() -> None:
    """``runtime.options`` exposes the tuning knobs."""
    from murmur.runtime import RuntimeOptions

    runtime = AgentRuntime()
    assert runtime.options == RuntimeOptions()
    assert runtime.options.timeout_seconds == 300.0
    assert runtime.options.retry_max_attempts == 1  # off by default


# ---------------------------------------------------------------------------
# Sync entry points
# ---------------------------------------------------------------------------


def test_run_sync_returns_typed_result(echo_agent: Agent, mock_backend) -> None:
    runtime = AgentRuntime(backend=mock_backend)
    result = runtime.run_sync(echo_agent, TaskSpec(input="hi"))
    assert result.is_ok()
    assert result.agent_name == echo_agent.name


def test_gather_sync_runs_each_task(echo_agent: Agent, mock_backend) -> None:
    runtime = AgentRuntime(backend=mock_backend)
    tasks = [TaskSpec(input=f"q{i}") for i in range(3)]
    results = runtime.gather_sync(echo_agent, tasks=tasks)
    assert len(results) == 3
    assert all(r.is_ok() for r in results)


async def test_run_sync_rejects_nested_call(echo_agent: Agent, mock_backend) -> None:
    """Calling ``run_sync`` from inside an event loop raises with a
    pointer to the async variant."""
    runtime = AgentRuntime(backend=mock_backend)
    with pytest.raises(RuntimeError, match="run_sync called from a running event loop"):
        runtime.run_sync(echo_agent, TaskSpec(input="hi"))


async def test_gather_sync_rejects_nested_call(echo_agent: Agent, mock_backend) -> None:
    runtime = AgentRuntime(backend=mock_backend)
    with pytest.raises(RuntimeError, match="gather_sync"):
        runtime.gather_sync(echo_agent, [TaskSpec(input="hi")])
