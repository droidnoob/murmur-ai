"""Unit tests for ``murmur.backends._collector.ResultCollector``."""

from __future__ import annotations

import asyncio

import pytest

from murmur.backends._collector import ResultCollector
from murmur.backends._inmemory_broker import InMemoryBroker
from murmur.core.errors import SpawnError
from murmur.messages import ResultMessage


def _ok_msg(batch_id: str, slot: int) -> ResultMessage:
    return ResultMessage(
        batch_id=batch_id,
        task_id=f"{batch_id}-{slot}",
        request_id="req",
        success=True,
        output_payload={"text": "ok"},
        agent_name="agent",
    )


async def _publish(broker: InMemoryBroker, topic: str, msg: ResultMessage) -> None:
    await broker.publish(topic, msg.model_dump_json().encode())
    # Two ticks so InMemoryBroker's create_task → handler → collector all run.
    await asyncio.sleep(0)
    await asyncio.sleep(0)


async def test_register_then_gather_returns_results_in_order() -> None:
    broker = InMemoryBroker()
    collector = ResultCollector(runtime_id="rt-1", broker=broker)
    await broker.start()
    await collector.start()

    collector.register("batch-1", expected=3)

    async def producer() -> None:
        await asyncio.sleep(0)
        for i in range(3):
            await _publish(broker, collector.reply_topic, _ok_msg("batch-1", i))

    asyncio.create_task(producer())
    slots = await collector.gather_batch(batch_id="batch-1")
    assert len(slots) == 3
    assert all(s is not None and s.success for s in slots)
    await broker.stop()


async def test_orphan_result_is_logged_and_dropped() -> None:
    broker = InMemoryBroker()
    collector = ResultCollector(runtime_id="rt-2", broker=broker)
    await broker.start()
    await collector.start()

    # No batch registered — collector must not crash.
    await _publish(broker, collector.reply_topic, _ok_msg("ghost", 0))
    await broker.stop()


async def test_gather_timeout_yields_none_for_missing_slots() -> None:
    broker = InMemoryBroker()
    collector = ResultCollector(runtime_id="rt-3", broker=broker)
    await broker.start()
    await collector.start()

    collector.register("batch-2", expected=2)
    await _publish(broker, collector.reply_topic, _ok_msg("batch-2", 0))
    slots = await collector.gather_batch(batch_id="batch-2", timeout=0.1)
    assert len(slots) == 2
    assert slots[0] is not None and slots[0].success
    assert slots[1] is None
    await broker.stop()


async def test_register_same_batch_twice_raises() -> None:
    broker = InMemoryBroker()
    collector = ResultCollector(runtime_id="rt-4", broker=broker)
    await broker.start()
    await collector.start()

    collector.register("dup", expected=1)
    with pytest.raises(SpawnError, match="already registered"):
        collector.register("dup", expected=1)
    await broker.stop()


async def test_await_handle_returns_single_envelope() -> None:
    broker = InMemoryBroker()
    collector = ResultCollector(runtime_id="rt-5", broker=broker)
    await broker.start()
    await collector.start()

    collector.register("h-1", expected=1)

    async def producer() -> None:
        await asyncio.sleep(0)
        await _publish(broker, collector.reply_topic, _ok_msg("h-1", 0))

    asyncio.create_task(producer())
    msg = await collector.await_handle(batch_id="h-1")
    assert msg is not None
    assert msg.success
    assert msg.task_id == "h-1-0"
    await broker.stop()
