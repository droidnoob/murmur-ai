"""Murmur — agents that move as one.

This module is the entire public API. PydanticAI and FastStream are
dependencies of Murmur, not of your code — keep all imports through
:mod:`murmur` and its submodules. The only exceptions are the migration
adapters in :mod:`murmur.interop`.

>>> from murmur import Agent, AgentRuntime, TaskSpec, TrustLevel
"""

from murmur.agent import Agent
from murmur.groups import AgentGroup, AgentTeam, Edge
from murmur.runtime import AgentRuntime
from murmur.templates import AgentTemplate
from murmur.types import (
    AgentContext,
    AgentHandle,
    AgentResult,
    FanOut,
    GroupResult,
    ResultMetadata,
    TaskSpec,
    TrustLevel,
)

__all__ = [
    "Agent",
    "AgentContext",
    "AgentGroup",
    "AgentHandle",
    "AgentResult",
    "AgentRuntime",
    "AgentTeam",
    "AgentTemplate",
    "Edge",
    "FanOut",
    "GroupResult",
    "ResultMetadata",
    "TaskSpec",
    "TrustLevel",
]

__version__ = "0.1.0"
