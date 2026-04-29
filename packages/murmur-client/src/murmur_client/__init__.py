"""``murmur-client`` — lightweight HTTP client for ``murmur.server.AgentServer``.

Depends only on ``httpx`` + ``pydantic``. Deliberately does **not**
import ``pydantic_ai`` / ``faststream`` or any of the runtime-side
machinery — the client knows the server URL, agent / group names, and
JSON schemas; nothing else.

>>> from murmur_client import MurmurClient
>>> async with MurmurClient("http://server:8421") as client:
...     result = await client.run("research-head", TaskSpec(input="..."))
"""

from murmur_client.client import MurmurClient, Run
from murmur_client.local import LocalClient

__all__ = ["LocalClient", "MurmurClient", "Run"]
