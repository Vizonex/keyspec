import pytest

from keyspec import Client


@pytest.mark.anyio
async def test_counter_1():
    async with Client(":memory:", auto_expire=True) as db:
        await db.set("i", "1", ttl=0.02)
        out = await db.incr("i")
        assert out == 1

        out = await db.decr("i")
        assert out is None


@pytest.mark.anyio
async def test_counter_2():
    async with Client(":memory:", auto_expire=True) as db:
        await db.set("i", "1", ttl=0.02)
        await db.incr("i")
        out = await db.incr("i")
        assert out == 2

        out = await db.decr("i")
        assert out == 1
