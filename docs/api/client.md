# Client

The `murmur-client` package ships separately and depends only on
`httpx` + `pydantic`. It deliberately does not import `pydantic_ai` /
`faststream` or any runtime-side machinery — the client knows the server
URL, agent / group names, and JSON schemas; nothing else.

```python
from murmur_client import LocalClient, MurmurClient, Run
```

Both client classes satisfy a shared `_RunBackend` Protocol — same
call surface, different transport.

## `MurmurClient`

HTTP client. Use when calling a remote `AgentServer` over the network.

::: murmur_client.MurmurClient
    options:
      heading_level: 3
      show_bases: false

## `LocalClient`

In-process client. Use when calling agents mounted in the same Python
process (typically via `AgentRouter`).

::: murmur.client.LocalClient
    options:
      heading_level: 3
      show_bases: false

## `Run`

Long-lived run handle returned by `client.submit()`.

::: murmur_client.Run
    options:
      heading_level: 3
      show_bases: false
