"""``murmur-client`` — lightweight HTTP client for ``murmur.server.AgentServer``.

Depends only on ``httpx`` + ``pydantic``. Deliberately does **not**
import ``pydantic_ai`` / ``faststream`` / ``murmur-ai`` or any of the
runtime-side machinery — the client knows the server URL, agent / group
names, and JSON schemas; nothing else. That keeps the install footprint
small enough to pull into a serverless function or a frontend service
without dragging the LLM stack along.

If you want an in-process companion (skip the HTTP round-trip when the
server lives in the same Python process), use
:class:`murmur.client.LocalClient` from the main ``murmur-ai`` package.

>>> from murmur_client import MurmurClient, TaskSpec
>>> async with MurmurClient("http://server:8421") as client:
...     result = await client.run("research-head", TaskSpec(input="..."))
"""

from murmur_client._wire import (
    AgentResult,
    AllAgentsFailedError,
    BudgetExceededError,
    ContextError,
    DepthLimitError,
    MurmurError,
    RegistryError,
    ResultMetadata,
    RunEvent,
    RunEventType,
    RunProgress,
    RunState,
    RunStatus,
    SpawnError,
    SpecValidationError,
    TaskSpec,
    ToolExecutionError,
    TopologyError,
    TrustViolationError,
)
from murmur_client.client import MurmurClient, Run

__all__ = [
    "AgentResult",
    "AllAgentsFailedError",
    "BudgetExceededError",
    "ContextError",
    "DepthLimitError",
    "MurmurClient",
    "MurmurError",
    "RegistryError",
    "ResultMetadata",
    "Run",
    "RunEvent",
    "RunEventType",
    "RunProgress",
    "RunState",
    "RunStatus",
    "SpawnError",
    "SpecValidationError",
    "TaskSpec",
    "ToolExecutionError",
    "TopologyError",
    "TrustViolationError",
]
