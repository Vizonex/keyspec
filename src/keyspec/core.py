import sys
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from functools import wraps
from logging import Logger
from pathlib import Path
from types import TracebackType
from typing import (
    Any,
    Concatenate,
    Generic,
    Literal,
    ParamSpec,
    TypeVar,
    overload,
)

from anyio import AsyncContextManagerMixin
from anyio_cysqlite import Connection, connect
from msgspec.msgpack import Decoder, Encoder

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self  # pragma: nocover

T = TypeVar("T")
D = TypeVar("D")
# used for making mini-wrappers.
P = ParamSpec("P")
R = TypeVar("R")


class Client(AsyncContextManagerMixin, Generic[T]):
    """A Client Connection inspired by aiocache that uses anyio, cysqlite and
    msgsepc to create and handle type-hintable and fast simplic key & value
    databases"""

    # NOTE: Datetime is already handled by cysqlite without surprise
    # deprecation let's hope it remains that way...
    KEY_VAL_TABLE = (
        "CREATE TABLE IF NOT EXISTS keyval "
        "(id INTEGER PRIMARY KEY, "
        "key TEXT UNIQUE, "
        "content BLOB, "
        "ttl DATETIME)"
    )

    __slots__ = (
        "__build_key",
        "_auto_expire",
        "_config",
        "_db",
        "_decoder",
        "_encoder",
        "_exception_handler",
        "_namespace",
        "_ttl",
    )

    @overload
    def __init__(
        self: "Client[T]",
        database: str | Path,
        type: type[T],
        ttl: float | timedelta | None = None,
        namespace: str | None = None,
        key_builder: Callable[[str, str | None], str] | None = None,
        auto_expire: bool = True,
        *,
        enc_hook: Callable[[Any], Any] | None = None,
        enc_decimal_format: Literal["string", "number"] = "string",
        enc_uuid_format: Literal["canonical", "hex"] = "canonical",
        enc_order: Literal[None, "deterministic", "sorted"] = None,
        dec_strict: bool = True,
        dec_hook: Callable[[type, Any], Any] | None = None,
        flags: int | None = None,
        timeout: float = 5.0,
        vfs: str | None = None,
        uri: bool = False,
        cached_statements: int = 100,
        extensions: bool = True,
        autoconnect: bool = True,
        log: Logger | None = None,
        exception_handler: Callable[
            [type[BaseException], BaseException, TracebackType, Logger], bool
        ]
        | None = None,
    ) -> None: ...

    @overload
    def __init__(
        self: "Client[T]",
        database: str | Path,
        type: T,
        ttl: float | timedelta | None = None,
        namespace: str | None = None,
        key_builder: Callable[[str, str | None], str] | None = None,
        auto_expire: bool = True,
        *,
        enc_hook: Callable[[Any], Any] | None = None,
        enc_decimal_format: Literal["string", "number"] = "string",
        enc_uuid_format: Literal["canonical", "hex"] = "canonical",
        enc_order: Literal[None, "deterministic", "sorted"] = None,
        dec_strict: bool = True,
        dec_hook: Callable[[type, Any], Any] | None = None,
        flags: int | None = None,
        timeout: float = 5.0,
        vfs: str | None = None,
        uri: bool = False,
        cached_statements: int = 100,
        extensions: bool = True,
        autoconnect: bool = True,
        log: Logger | None = None,
        exception_handler: Callable[
            [type[BaseException], BaseException, TracebackType, Logger], bool
        ]
        | None = None,
    ) -> None: ...

    @overload
    def __init__(
        self: "Client[Any]",
        database: str | Path,
        type: None = None,
        ttl: float | timedelta | None = None,
        namespace: str | None = None,
        key_builder: Callable[[str, str | None], str] | None = None,
        auto_expire: bool = True,
        *,
        enc_hook: Callable[[Any], Any] | None = None,
        enc_decimal_format: Literal["string", "number"] = "string",
        enc_uuid_format: Literal["canonical", "hex"] = "canonical",
        enc_order: Literal[None, "deterministic", "sorted"] = None,
        dec_strict: bool = True,
        dec_hook: Callable[[type, Any], Any] | None = None,
        flags: int | None = None,
        timeout: float = 5.0,
        vfs: str | None = None,
        uri: bool = False,
        cached_statements: int = 100,
        extensions: bool = True,
        autoconnect: bool = True,
        log: Logger | None = None,
        exception_handler: Callable[
            [type[BaseException], BaseException, TracebackType, Logger], bool
        ]
        | None = None,
    ) -> None: ...

    def __init__(
        self,
        database: str | Path,
        type: type[T] | None = None,
        ttl: float | timedelta | None = None,
        namespace: str | None = None,
        key_builder: Callable[[str, str | None], str] | None = None,
        auto_expire: bool = True,
        *,
        enc_hook: Callable[[Any], Any] | None = None,
        enc_decimal_format: Literal["string", "number"] = "string",
        enc_uuid_format: Literal["canonical", "hex"] = "canonical",
        enc_order: Literal[None, "deterministic", "sorted"] = None,
        dec_strict: bool = True,
        dec_hook: Callable[[type, Any], Any] | None = None,
        flags: int | None = None,
        timeout: float = 5.0,
        vfs: str | None = None,
        uri: bool = False,
        cached_statements: int = 100,
        extensions: bool = True,
        autoconnect: bool = True,
        log: Logger | None = None,
        exception_handler: Callable[
            [type[BaseException], BaseException, TracebackType, Logger], bool
        ]
        | None = None,
    ) -> None:
        # Rather than hold onto a bunch of attributes lets
        # put these into a factory for now...

        self._config = dict(
            database=database,
            flags=flags,
            timeout=timeout,
            vfs=vfs,
            uri=uri,
            cached_statements=cached_statements,
            extensions=extensions,
            autoconnect=autoconnect,
            log=log,
            exception_handler=exception_handler,
        )

        self._encoder = Encoder(
            enc_hook=enc_hook,
            decimal_format=enc_decimal_format,
            uuid_format=enc_uuid_format,
            order=enc_order,
        )
        if type is not None:
            self._decoder: Decoder[T] = Decoder(
                type, strict=dec_strict, dec_hook=dec_hook
            )
        else:
            self._decoder: Decoder[T] = Decoder(
                strict=dec_strict, dec_hook=dec_hook
            )

        self._db: Connection | None = None
        self._exception_handler = exception_handler

        self._ttl = self._convert_ttl(ttl)
        self._namespace = namespace
        self.__build_key = key_builder or self._build_key
        self._auto_expire = auto_expire


    @property
    def default_namespace(self) -> str | None:
        """the default namespace, if provided to the client."""
        return self._namespace


    # Based off aiocache but there is now typehinting, your welcome :)
    def _build_key(self, key: str, namespace: str | None = None):
        if (ns := namespace or self._namespace) is not None:
            return "{}{}".format(ns, key)
        return key

    def _now(self) -> datetime:
        """Simple shortcut for getting to `datetime.now()`
        this could also be subclassed and modified if needed."""
        return datetime.now()

    def _convert_ttl(self, ttl: float | timedelta | None) -> timedelta | None:
        if ttl is not None:
            return (
                timedelta(seconds=ttl)
                if not isinstance(ttl, timedelta)
                else ttl
            )

    def _next_ttl(self, ttl: float | timedelta | None) -> datetime | None:
        """:returns: next ttl."""
        _ttl = self._convert_ttl(ttl) or self._ttl
        if _ttl is not None:
            return self._now() + _ttl

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncIterator[Self]:
        async with await connect(**self._config) as conn:
            self._db = conn
            async with self._db.atomic():
                await self._db.execute(self.KEY_VAL_TABLE)
            yield self
            await self.close()

    async def close(self) -> None:
        """Performs closure and checks if auto-expire is set.
        If this was the case, then the cache is cleansed based,
        off the provided namespace. Otherwise, expire everything.
        """
        if self._auto_expire:
            await self.expire()

    async def set(
        self,
        key: str,
        value: T,
        namespace: str | None = None,
        ttl: float | timedelta | None = None,
    ):
        """Sets a value to set or be updated.

        :param key: the key for the value.
        :param value: the value to set up.
        :param namespace: namespace of the given entry (if any).
        :param ttl: the expiration date otherwise it shall
            never expire unless the client was given a default ttl
            of some kind.
        """
        ns = namespace if namespace is not None else self._namespace
        ns_key = self.__build_key(key, ns)
        async with self._db.atomic():
            await self._db.execute(
                "INSERT INTO keyval (key, content, ttl) VALUES (?, ?, ?) "
                "ON CONFLICT (key) "
                "DO UPDATE SET content=excluded.content, ttl=excluded.ttl",
                (ns_key, self._encoder.encode(value), self._next_ttl(ttl)),
            )

    async def get(self, key: str, namespace: str | None = None) -> T | None:
        """Gets a single entry in the cache/databse.
        :param key: the key where the entry is.
        :param namespace: the namespace for that given entry if provided."""
        row = await self._db.execute_one(
            "SELECT content FROM keyval WHERE key=?",
            (self.__build_key(key, namespace),),
        )
        if row is not None:
            return self._decoder.decode(row["content"])

    async def search_iter(
        self, key: str, namespace: str | None = None
    ) -> AsyncIterator[tuple[str, T]]:
        """Searches all keys by given name or namespace"""
        async with await self._db.execute(
            "SELECT key, content FROM keyval WHERE key LIKE ?",
            (f"%{self.__build_key(key, namespace)}",),
        ) as cursor:
            async for row in cursor:
                yield row[0], self._decoder.decode(row[1])

    async def get_iter(
        self, namespace: str | None = None
    ) -> AsyncIterator[tuple[str, T]]:
        """Searches all keys by given name or namespace"""
        namespace = namespace or self._namespace
        if namespace:
            async with await self._db.execute(
                "SELECT key, content FROM keyval WHERE key LIKE ?",
                (f"%{namespace}",),
            ) as cursor:
                async for row in cursor:
                    yield row[0], self._decoder.decode(row[1])
        else:
            async with await self._db.execute(
                "SELECT key, content FROM keyval"
            ) as cursor:
                async for row in cursor:
                    yield row[0], self._decoder.decode(row[1])

    async def search(
        self, key: str, namespace: str | None = None
    ) -> dict[str, T]:
        """Gets all keys & values searched for, you can also use key as a
        namespace or leave key as an empty string if needed, know that
        `get_all()` can consume more memory if your database is larger.
        alternatively use `search_iter()` for larger loads"""
        return {k: v async for k, v in self.search_iter(key, namespace)}

    async def get_all(self, namespace: str | None = None) -> dict[str, T]:
        """Gets all keys & values searched for by namespace

        :param namespace: namespace the keys are located in
            if there isn't any all entries by the client's namespace
            or everything is loaded."""
        return {k: v async for k, v in self.get_iter(namespace)}

    async def delete(self, key: str, namespace: str | None = None) -> T | None:
        """Deletes values by exact key name.

        :param key: the key to be deleted
        :param namespace: the namespace the key is located in (if any)"""
        async with self._db.atomic():
            await self._db.execute_one(
                "DELETE FROM keyval WHERE key=?",
                (self.__build_key(key, namespace),),
            )

    async def expire_all(self) -> None:
        """Removes everything that has passed it's expiration date."""
        async with self._db.atomic():
            await self._db.execute_one(
                "DELETE FROM keyval WHERE ttl < ?", (self._now(),)
            )

    async def expire(self, namespace: str | None = None) -> None:
        """Removes everything by namespace that has passed it's expiration
        date.

        :param namespace: the namespace of keys to expire
            default namespace name is chosen if no custom namespace is provided
            otherwise it expires everything.
        """
        ns = namespace or self._namespace
        now = self._now()
        async with self._db.atomic():
            if ns:
                await self._db.execute(
                    "DELETE FROM keyval WHERE ttl < ? AND key LIKE ?",
                    (now, f"%{ns}"),
                )
            else:
                await self._db.execute(
                    "DELETE FROM keyval WHERE ttl < ?", (now,)
                )

    async def wipe(self):
        """cleans out the whole database/cache.

        **WARNING, THIS FUNCTION IS DANGEROUS!**
        """
        async with self._db.atomic():
            await self._db.execute_one("DROP TABLE IF EXISTS keyval;")




@overload
def cache(
    database: str | Path,
    type: None = ...,
    ttl: float | timedelta | None = None,
    namespace: str | None = None,
    key_builder: Callable[[str, str | None], str] | None = None,
    auto_expire: bool = True,
    *,
    enc_hook: Callable[[Any], Any] | None = None,
    enc_decimal_format: Literal["string", "number"] = "string",
    enc_uuid_format: Literal["canonical", "hex"] = "canonical",
    enc_order: Literal[None, "deterministic", "sorted"] = None,
    dec_strict: bool = True,
    dec_hook: Callable[[type, Any], Any] | None = None,
    flags: int | None = None,
    timeout: float = 5.0,
    vfs: str | None = None,
    uri: bool = False,
    cached_statements: int = 100,
    extensions: bool = True,
    autoconnect: bool = True,
    log: Logger | None = None,
    exception_handler: Callable[
        [type[BaseException], BaseException, TracebackType, Logger], bool
    ]
    | None = None,
) -> Callable[
    [Callable[Concatenate[Client[Any], P], Coroutine[Any, Any, R]]],
    Callable[P, Coroutine[Any, Any, R]],
]: ...


@overload
def cache(
    database: str | Path,
    type: type[T],
    ttl: float | timedelta | None = None,
    namespace: str | None = None,
    key_builder: Callable[[str, str | None], str] | None = None,
    auto_expire: bool = True,
    *,
    enc_hook: Callable[[Any], Any] | None = None,
    enc_decimal_format: Literal["string", "number"] = "string",
    enc_uuid_format: Literal["canonical", "hex"] = "canonical",
    enc_order: Literal[None, "deterministic", "sorted"] = None,
    dec_strict: bool = True,
    dec_hook: Callable[[type, Any], Any] | None = None,
    flags: int | None = None,
    timeout: float = 5.0,
    vfs: str | None = None,
    uri: bool = False,
    cached_statements: int = 100,
    extensions: bool = True,
    autoconnect: bool = True,
    log: Logger | None = None,
    exception_handler: Callable[
        [type[BaseException], BaseException, TracebackType, Logger], bool
    ]
    | None = None,
) -> Callable[
    [Callable[Concatenate[Client[T], P], Coroutine[Any, Any, R]]],
    Callable[P, Coroutine[Any, Any, R]],
]: ...

def cache(
    database: str | Path,
    type: type[T] | None = None,
    ttl: float | timedelta | None = None,
    namespace: str | None = None,
    key_builder: Callable[[str, str | None], str] | None = None,
    auto_expire: bool = True,
    **kw,
) -> Callable[
    [Callable[Concatenate[Client[T], P], Coroutine[Any, Any, R]]],
    Callable[P, Coroutine[Any, Any, R]],
]:
    """Provides a lazy factory for utilizing an asynchronous cache accross
    different functions."""

    def decorator(
        func: Callable[Concatenate[Client[T], P], Coroutine[Any, Any, R]],
    ) -> Callable[P, Coroutine[Any, Any, R]]:
        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs):
            async with Client(
                database, type, ttl, namespace, key_builder, auto_expire, **kw
            ) as client:
                return await func(client, *args, **kwargs)

        return wrapper

    return decorator
