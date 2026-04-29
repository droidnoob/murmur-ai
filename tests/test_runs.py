"""Unit tests for ``murmur.runs`` — value types + ``InMemoryRunStore``.

The InMemoryRunStore-specific behaviour is verified via the shared
:class:`tests.contracts.runstore_contract.RunStoreContract` suite — the
same suite every persistent store (``SQLiteRunStore`` /
``RocksDBRunStore`` / ``RedisRunStore``) is expected to pass.
"""

from __future__ import annotations

import pytest
from tests.contracts.runstore_contract import RunStoreContract

from murmur.runs import InMemoryRunStore, RunStore


class TestInMemoryRunStore(RunStoreContract):
    @pytest.fixture
    async def store(self) -> RunStore:
        return InMemoryRunStore()
