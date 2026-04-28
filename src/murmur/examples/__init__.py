"""Reference Pydantic models used by the example specs under ``specs/``.

Users normally point YAML ``output_type`` / ``input_type`` at their own
package (e.g. ``mypkg.outputs.ResearchFinding``); this module exists so
the example spec under ``specs/agents/researcher.yaml`` resolves
out-of-the-box and ``murmur validate specs/`` has something concrete to
chew on. Treat the contents as a copy-and-adapt template, not a stable
public API.
"""

from murmur.examples.types import ResearchFinding, ResearchQuestion

__all__ = ["ResearchFinding", "ResearchQuestion"]
