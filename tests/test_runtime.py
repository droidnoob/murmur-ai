"""Smoke tests for ``murmur.AgentRuntime``."""

from __future__ import annotations

import pytest
from tests.conftest import MockBackend

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


def test_no_broker_picks_thread_backend() -> None:
    runtime = AgentRuntime()
    assert runtime.backend.__class__.__name__ == "ThreadBackend"


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
