from collections.abc import AsyncIterator
from typing import Any

from anyio_cysqlite import Connection, connect

from .base import T
from .sqlite_base import KEYVAL_CREATE_TABLE, BaseSqlite, touch


class DB(BaseSqlite[T]):
    """A Cysqlite Database implemented with anyio-cysqlite."""

    async def connect(self) -> None:
        await touch(self._path)
        self._db: Connection = await connect(self._path, **self._kw)
        await self._db.executescript(KEYVAL_CREATE_TABLE)

    async def execute_iter(
        self, sql: str, params: Any = ()
    ) -> AsyncIterator[tuple[T, str]]:
        async with self._db.atomic():
            async with await self._db.execute(sql, params) as cursor:
                async for row in cursor:
                    yield (row['key'], self.decode(row['value']))

    async def execute_one(self, sql: str, params: Any = ()) -> T | None:
        async with self._db.atomic():
            result = await self._db.execute_one(sql, params)
            if result is not None:
                return self.decode(result[0])

    async def execute_raw(self, sql: str, params: Any = ()) -> Any | None:
        async with self._db.atomic():
            result = await self._db.execute_one(sql, params)
            if result is not None:
                return result[0]

    async def execute(self, sql: str, params: Any = ()) -> None:
        async with self._db.atomic():
            await self._db.execute_one(sql, params)

    async def execute_script(self, sql: str) -> None:
        async with self._db.atomic():
            await self._db.executescript(sql)

    async def _close(self):
        await self._db.close()
        # Cleanup for next run.
        self._db = None

