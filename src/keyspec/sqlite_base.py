import os
from abc import abstractmethod
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path
from typing import Any, AnyStr, cast

from anyio.to_thread import run_sync

from .base import AbstractDecoder, AbstractEncoder, BaseDBM, T

# Based off the new dmb.sqlite extension in python.

# SQL Commands

KEYVAL_CREATE_TABLE = (
    "CREATE TABLE IF NOT EXISTS keyval "
    "(id INTEGER PRIMARY KEY AUTOINCREMENT, "
    "key TEXT UNIQUE, "
    "value BLOB, "
    "ttl DATETIME,"
    "count INTEGER NOT NULL DEFAULT 0)"
)

KEYVAL_SELECT_KEY_LIKE = "SELECT key, value FROM keyval WHERE key LIKE ?"
KEYVAL_SELECT_ALL = "SELECT key, value FROM keyval"

KEYVAL_SET = (
    "INSERT INTO keyval (key, value, ttl) VALUES (?, ?, ?) "
    "ON CONFLICT (key) "
    "DO UPDATE SET value=excluded.value, ttl=excluded.ttl"
)
KEYVAL_GET = "SELECT value FROM keyval WHERE key=?"

KEYVAL_DELETE = "DELETE FROM keyval WHERE key=?"

KEYVAL_EXPIRE = "DELETE FROM keyval WHERE ttl < ?"

KEYVAL_EXPIRE_LIKE = "DELETE FROM keyval WHERE ttl < ? AND key LIKE ?"
KEYVAL_INCR = "UPDATE keyval SET count = count + 1 WHERE key=? RETURNING count"

KEYVAL_DEC = (
    "UPDATE keyval SET count = count - 1 WHERE key=?"
    " AND count > 0 RETURNING count"
)
KEYVAL_REORGANIZE = "VACUUM"


def _normalize_uri(path):
    path = Path(path)
    uri = path.absolute().as_uri()
    while "//" in uri:
        uri = uri.replace("//", "/")
    return uri


async def touch(path: AnyStr | Path, mode: int = 0o666, exist_ok: bool = True):
    """
    Create this file with the given access mode, if it doesn't exist.

    Warning
    -------
    **This function may not be threadsafe!**
    """
    path = Path(path)
    # set cancellable to False because touch can be unpredictable
    # and the os module is known for not being threadsafe.
    return await run_sync(path.touch, mode, exist_ok)


class BaseSqlite(BaseDBM[T]):
    """Basic Sqlite3 Abstract class for usage with
    sqlite-anyio, anyio-cysqlite and more.
    """

    __slots__ = ("_db", "_path")

    def __init__(
        self,
        path: str | bytes | Path,
        dec: AbstractDecoder[T] | None = None,
        enc: AbstractEncoder | None = None,
        default_ttl: float | timedelta | None = None,
        auto_expire: bool = True,
        delim: str = ".",
    ):
        super().__init__(dec, enc, default_ttl, auto_expire, delim)

        self._path = os.fsdecode(path)
        self._db = None

    @abstractmethod
    async def execute_iter(
        self, sql: str, params: Any
    ) -> AsyncIterator[tuple[str, T]]:
        pass

    @abstractmethod
    async def execute_one(self, sql: str, params: Any = ()) -> T | None:
        pass

    @abstractmethod
    async def execute_raw(self, sql: str, params: Any = ()) -> Any | None:
        pass

    @abstractmethod
    async def execute(self, sql: str, params: Any = ()) -> None: ...

    @abstractmethod
    async def execute_script(self, sql: str) -> None: ...

    async def reorganize(self) -> None:
        """WARNING: May result in devistating affects."""
        await self.execute_script(KEYVAL_REORGANIZE)

    async def set(
        self, key: str, value: T, ttl: float | timedelta | None = None
    ) -> None:
        await self.execute(KEYVAL_SET, (key, self.encode(value), ttl))

    async def get(self, key: str) -> T | None:
        return await self.execute_one(KEYVAL_GET, (key,))

    async def search_iter(self, key: str) -> AsyncIterator[tuple[str, T]]:
        async for i in self.execute_iter(KEYVAL_SELECT_KEY_LIKE, key):
            yield i

    async def get_iter(self) -> AsyncIterator[tuple[str, T]]:
        async for i in self.execute_iter(KEYVAL_SELECT_ALL):
            yield i

    async def delete(self, key: str) -> None:
        await self.execute(KEYVAL_DELETE, (key,))

    async def expire_all(self):
        await self.execute_script(KEYVAL_EXPIRE)

    async def expire(self, key: str | None = None):
        now = self.now()
        if key:
            return await self._db.execute(
                    KEYVAL_EXPIRE_LIKE,
                    (now, f"%{key}"),
                )
        else:
            await self.expire_all()

    async def incr(self, key: str) -> int | None:
        if row := await self.execute_one(KEYVAL_INCR, (key,)):
            return cast(int, row)

    async def decr(self, key: str) -> int | None:
        if row := await self.execute_one(KEYVAL_DEC, (key,)):
            return cast(int, row)
