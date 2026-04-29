"""RocksDBRunStore — runs the shared :class:`RunStoreContract` suite."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from tests.contracts.runstore_contract import RunStoreContract

from murmur.runs import RocksDBRunStore, RunState, RunStore


class TestRocksDBRunStore(RunStoreContract):
    @pytest.fixture
    async def store(self, tmp_path: Path) -> AsyncIterator[RunStore]:
        s = RocksDBRunStore(tmp_path / "runs.rdb")
        try:
            yield s
        finally:
            await s.close()


async def test_persistence_across_reopen(tmp_path: Path) -> None:
    """A persisted run is recoverable after closing and reopening the store."""
    path = tmp_path / "runs.rdb"

    s1 = RocksDBRunStore(path)
    await s1.create("abc", target="x")
    await s1.set_state("abc", RunState.RUNNING)
    await s1.close()

    s2 = RocksDBRunStore(path)
    status = await s2.get_status("abc")
    assert status.run_id == "abc"
    assert status.state is RunState.RUNNING
    await s2.close()
