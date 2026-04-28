"""Stable Pydantic models referenced by YAML class paths in tests.

Pytest can import a test module under two different paths (via the
package layout vs the file path), giving you two distinct class objects
for what looks like the same class. ``YamlRegistry`` then resolves the
class via ``importlib`` and the result fails ``is``-identity checks
against the test module's local copy. Keeping the fixture types here —
in a non-test module — sidesteps the issue: there's exactly one import
path, so identity holds.
"""

from __future__ import annotations

from pydantic import BaseModel


class FixtureOutput(BaseModel):
    text: str


class FixtureInput(BaseModel):
    query: str


__all__ = ["FixtureInput", "FixtureOutput"]
