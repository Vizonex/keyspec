import anyio
import pytest

from keyspec import Client


@pytest.mark.anyio
async def test_expiration():
    async with Client(":memory:") as db:
        await db.set("i", "1", ttl=0.02)
        val = await db.get("i")
        assert val == "1"
        await anyio.sleep(0.02)
        await db.expire()
        assert (await db.get("i")) is None
