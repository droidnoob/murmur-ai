"""Murmur domain errors.

All errors raised from Murmur internals derive from :class:`MurmurError`.
Never raise raw ``Exception`` or ``ValueError`` from ``core/`` — pick the most
specific subclass below, or add a new one if a genuinely new failure category
appears.
"""


class MurmurError(Exception):
    """Base class for all Murmur errors."""


class SpawnError(MurmurError):
    """Agent failed to spawn or its execution backend errored."""


class ToolExecutionError(MurmurError):
    """A tool call requested by the agent failed during execution."""


class ContextError(MurmurError):
    """Context preparation by a ``ContextPasser`` failed."""


class BudgetExceededError(MurmurError):
    """Token, cost, or wall-clock budget was exceeded."""


class DepthLimitError(MurmurError):
    """Cascading-spawn depth limit was reached."""


class SpecValidationError(MurmurError):
    """An agent or group spec failed validation."""


class RegistryError(MurmurError):
    """A spec was not found in the registry, or the registry rejected a write."""


class TrustViolationError(MurmurError):
    """A tool call was denied because the agent's trust level forbids it."""


class AllAgentsFailedError(MurmurError):
    """Every result in a fan-out tier failed; the downstream mapper was not called."""


class TopologyError(SpecValidationError):
    """An ``AgentGroup`` topology is invalid — cycle, dangling reference, or
    incompatible types across an edge."""
