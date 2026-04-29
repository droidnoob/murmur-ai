# murmur-client

Lightweight Python client for [Murmur](https://github.com/droidnoob/murmur-ai)
agent servers. Two transports:

- **HTTP** — `MurmurClient(server_url=...)` talks to a remote `AgentServer` /
  `AgentRouter` over `httpx`.
- **In-process** — `LocalClient(server=...)` dispatches against a local
  `AgentRuntime` without an HTTP round-trip; useful when both producer and
  consumer live in the same ASGI app.

Both classes satisfy the same calling Protocol; generic code accepts either.

```python
from murmur_client import MurmurClient, LocalClient
```

See [Murmur's docs](https://github.com/droidnoob/murmur-ai/blob/main/docs.md)
for full usage.
