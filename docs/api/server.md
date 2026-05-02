# Server

HTTP-facing surface for hosted agents. `AgentServer` is the standalone
form (`murmur serve`); `AgentRouter` is the embedded form mounted on a
user-supplied FastAPI app.

```python
from murmur.server import AgentRouter, AgentServer, ErrorResponse
```

The `[server]` extra is required for these imports — see
[Installation](../getting-started/installation.md#server-extras).

## `AgentServer`

::: murmur.server.AgentServer
    options:
      heading_level: 3
      show_bases: false

## `AgentRouter`

::: murmur.server.AgentRouter
    options:
      heading_level: 3
      show_bases: false

## `ErrorResponse`

::: murmur.server.ErrorResponse
    options:
      heading_level: 3
      show_bases: false

`AgentRouter.install_exception_handlers(app)` — classmethod, called once
on the host FastAPI app to wire Murmur's domain errors to the HTTP
status codes in `server/errors.py`. Decision D13.
