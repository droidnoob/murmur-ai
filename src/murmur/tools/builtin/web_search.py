"""Built-in web_search tool — Phase 1 stub.

Real implementation (provider selection, rate limiting, caching) lands once
the runtime dispatch loop is wired up.
"""

from __future__ import annotations


async def web_search(query: str, *, max_results: int = 5) -> list[str]:
    """Search the web. Returns a list of snippet strings."""
    raise NotImplementedError(
        "web_search — Phase 1 stub; real provider integration pending"
    )


__all__ = ["web_search"]
