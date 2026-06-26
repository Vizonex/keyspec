import pytest
from msgspec import Struct

from keyspec import Client, cache


class User(Struct):
    name: str
    password: str


factory = cache(
    "data.db",
    User,
)


@factory
async def instert(client: Client[User], username: str, password: str) -> None:
    await client.set(username, User(username, password))


@factory
async def get(client: Client[User], username: str) -> User | None:
    return await client.get(username)


@factory
async def get_all(client: Client[User]) -> dict[str, User]:
    return await client.get_all()


@factory
async def wipe(client: Client[User]) -> None:
    await client.wipe()


@pytest.mark.anyio
async def test_example() -> None:
    await instert("user", "pass")
    await instert("user1", "password")
    data = await get_all()

    assert data["user"] == User(name="user", password="pass")
    assert data["user1"] == User(name="user1", password="password")
    await wipe()

    a = await get_all()
    assert not a
