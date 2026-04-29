"""Built-in web_search tool — placeholder.

Real implementation (provider selection, rate limiting, caching) lands once
a search provider is selected.
"""

from __future__ import annotations


async def web_search(query: str, *, max_results: int = 5) -> list[str]:
    """Search the web. Returns a list of snippet strings."""
    raise NotImplementedError("web_search — stub; no provider integration yet")


__all__ = ["web_search"]
