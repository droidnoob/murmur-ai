"""``murmur-client`` — lightweight HTTP client for ``murmur.server.AgentServer``.

Per Addendum 2 §"Client / Server Split": this package depends only on
``httpx`` + ``pydantic``. It deliberately does **not** import
``pydantic_ai`` / ``faststream`` or any of the runtime-side machinery —
the client knows the server URL, agent / group names, and JSON schemas;
nothing else.

Phase 1 ships this code under ``src/murmur_client/`` of the main repo;
splitting it into a separately-distributable wheel (``pip install
murmur-client``) is a packaging change tracked in the phase-1 checklist.

>>> from murmur_client import MurmurClient
>>> async with MurmurClient("http://server:8421") as client:
...     result = await client.run("research-head", TaskSpec(input="..."))
"""

from murmur_client.client import MurmurClient, Run

__all__ = ["MurmurClient", "Run"]
