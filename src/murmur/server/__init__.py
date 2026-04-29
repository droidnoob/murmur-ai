"""``murmur.server`` — HTTP-facing surface for hosted agents.

Public:

- :class:`AgentServer` — register agents and groups, expose them via FastAPI.
- :class:`murmur.server.errors.ErrorResponse` — wire shape for HTTP errors.

Lazy-imported so the rest of ``murmur`` stays installable without the
``[server]`` extra. Importing :class:`AgentServer` requires ``fastapi`` /
``uvicorn`` / ``sse-starlette``.
"""

from murmur.server.app import AgentServer
from murmur.server.errors import ErrorResponse
from murmur.server.router import AgentRouter

__all__ = ["AgentRouter", "AgentServer", "ErrorResponse"]
