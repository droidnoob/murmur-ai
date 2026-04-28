"""Migration adapters between Murmur and the underlying libraries.

Only this package may import :mod:`pydantic_ai` or :mod:`faststream` directly.
Application code should depend on :mod:`murmur` instead.
"""

from murmur.interop.faststream import as_faststream_handler
from murmur.interop.pydantic_ai import from_pydantic_ai

__all__ = ["as_faststream_handler", "from_pydantic_ai"]
