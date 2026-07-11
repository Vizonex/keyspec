import os

import pytest
from msgspec import Struct
from msgspec.msgpack import Decoder

from keyspec.sqlite_base import BaseSqlite


class User(Struct):
    name: str
    password: str

@pytest.fixture
async def user_db(dbm: type[BaseSqlite[User]]):
    async with dbm("data.db", dec=Decoder(User)) as db:
        yield db
    os.remove("data.db")

@pytest.mark.anyio
async def test_example(user_db: BaseSqlite[User]) -> None:

    await user_db.set("user", User("user", "pass"))
    await user_db.set("user1", User("user1", "password"))
    data = await user_db.get_all()

    assert data["user"] == User(name="user", password="pass")
    assert data["user1"] == User(name="user1", password="password")

@pytest.mark.anyio
async def test_namespaces(user_db: BaseSqlite[User]) -> None:

    await user_db["namespace"].set("user", User("user", "pass"))
    user = await user_db["namespace"].get("user")
    assert user.password == "pass"


