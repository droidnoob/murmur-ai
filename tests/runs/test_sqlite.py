"""SQLiteRunStore — runs the shared :class:`RunStoreContract` suite."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from tests.contracts.runstore_contract import RunStoreContract

from murmur.runs import RunStore, SQLiteRunStore


class TestSQLiteRunStore(RunStoreContract):
    @pytest.fixture
    async def store(self, tmp_path: Path) -> AsyncIterator[RunStore]:
        s = SQLiteRunStore(tmp_path / "runs.db")
        try:
            yield s
        finally:
            await s.close()


async def test_lazy_attr_access_via_runs_namespace(tmp_path: Path) -> None:
    """``from murmur.runs import SQLiteRunStore`` resolves through ``__getattr__``."""
    from murmur import runs

    store = runs.SQLiteRunStore(tmp_path / "runs.db")
    await store.create("abc", target="x")
    status = await store.get_status("abc")
    assert status.run_id == "abc"
    await store.close()
