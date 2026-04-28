"""Reference Pydantic models for the bundled example specs."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ResearchQuestion(BaseModel):
    """One sub-question handed to a research minion."""

    question: str
    search_terms: list[str] = Field(default_factory=list)


class ResearchFinding(BaseModel):
    """One agent's structured answer to a single :class:`ResearchQuestion`."""

    question: str
    answer: str
    confidence: float = Field(ge=0.0, le=1.0)
    sources: list[str] = Field(default_factory=list)
    key_facts: list[str] = Field(default_factory=list)


__all__ = ["ResearchFinding", "ResearchQuestion"]
