"""In-process client for :class:`murmur.AgentRuntime` / :class:`AgentServer`.

Use :class:`murmur.client.LocalClient` when producer and consumer live in
the same Python process and you want to skip an HTTP round-trip. The
calling surface mirrors the HTTP :class:`murmur_client.MurmurClient`
exactly so generic code can accept either.

>>> from murmur import AgentRuntime
>>> from murmur.server import AgentServer
>>> from murmur.client import LocalClient
>>>
>>> server = AgentServer(runtime=AgentRuntime())
>>> server.register(my_agent)
>>> async with LocalClient(server=server) as client:
...     result = await client.run("my_agent", TaskSpec(input="..."))
"""

from __future__ import annotations

from murmur.client.local import LocalClient, LocalRun

__all__ = ["LocalClient", "LocalRun"]
