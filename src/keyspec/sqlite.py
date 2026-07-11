import sqlite3
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Any

from sqlite_anyio import Connection, connect

from .base import T
from .sqlite_base import KEYVAL_CREATE_TABLE, BaseSqlite, touch


class DB(BaseSqlite[T]):
    """A Database implemented with sqlite-anyio"""
    @asynccontextmanager
    async def __autocommit(self):
        async with await self._db.cursor() as cursor:
            try:
                yield cursor
            except BaseException as e:
                await self._db.rollback()
                raise e
            else:
                await self._db.commit()

    async def connect(self) -> None:
        await touch(self._path)
        self._db: Connection = await connect(self._path, **self._kw)
        # Attempt to go for journal_mode=wal, ignore if this fails.
        with suppress(sqlite3.OperationalError):
            await self.execute("PRAGMA journal_mode = wal")
        await self.execute(KEYVAL_CREATE_TABLE)

    async def execute_iter(
        self, sql: str, params: Any = ()
    ) -> AsyncIterator[tuple[T, str]]:
        # __autocommit isn't needed since were just grabbing data.
        async with await self._db.cursor() as cursor:
            async with await cursor.execute(sql, params) as cursor:
                # XXX: Nasty little annoyance with iterators.
                for row in cursor._real_cursor:
                    yield (row[0], self.decode(row[1]))

    async def execute_one(self, sql: str, params: Any = ()) -> T | None:
        async with self.__autocommit() as cursor:
            async with await cursor.execute(sql, params) as r_cursor:
                result = await r_cursor.fetchone()
                if result is not None:
                    return self.decode(result[0])

    async def execute_raw(self, sql: str, params: Any = ()) -> Any | None:
        async with self.__autocommit() as cursor:
            async with await cursor.execute(sql, params) as r_cursor:
                result = await r_cursor.fetchone()
                if result is not None:
                    return result[0]

    async def execute(self, sql: str, params: Any = ()) -> None:
        async with self.__autocommit() as cursor:
            await cursor.execute(sql, params)

    async def execute_script(self, sql: str) -> None:
        async with self.__autocommit() as cursor:
            await cursor.executescript(sql)

    async def _close(self):
        await self._db.close()
        # Cleanup for next run.
        self._db = None

