"""
base
----

Allows for multiple different database extensions to be created.
"""

import sys
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable, Coroutine
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Concatenate, Generic, ParamSpec, TypeVar

from msgspec import json, msgpack

# Based on newer sqlite dbm extension (There will be more implementatons
# in the future)

T = TypeVar("T")
R = TypeVar("R")
P = ParamSpec("P")

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self  # pragma: nocover

if sys.version_info >= (3, 12):
    from collections.abc import Buffer
else:
    from typing_extensions import Buffer


class AbstractDecoder(ABC, Generic[T]):
    @abstractmethod
    def decode(self, buf: Buffer, /) -> T:
        pass


AbstractDecoder.register(json.Decoder)
AbstractDecoder.register(msgpack.Decoder)


class AbstractEncoder(ABC):
    @abstractmethod
    def encode(obj: Any) -> bytes:
        pass


AbstractEncoder.register(json.Encoder)
AbstractEncoder.register(msgpack.Encoder)


class NSDBM(Generic[T]):
    """
    A Database Model with Namespace and key (This can also be used recursively)
    """

    def __init__(self, dbm: "BaseDBM[T]", ns_path: list[str]):
        self._dbm = dbm
        self._ns_path = ns_path
        self._delim = dbm._delim
        self._namespace = self._delim.join(ns_path)
        super().__init__()

    def _next_ns(self, ns: str):
        # Make a copy with ns_path copied so it can't be manipulated
        # in the next state.
        return NSDBM(self._dbm, [*self._ns_path, ns])

    @property
    def namespace(self):
        """Obtains current path of the namespace made"""
        return self._namespace

    def make_key(self, key: str) -> str:
        """Creates a new key for the query used.

        `<namespace><dbm.delim><key>`
        """
        return f"{self._namespace}{self._delim}{key}"

    async def set(
        self,
        key: str,
        value: T,
        ttl: float | timedelta | None = None,
    ) -> None:
        """Sets a value to set or be updated.

        :param key: the key for the value.
        :param value: the value to set up.
        :param ttl: the expiration date otherwise it shall
            never expire unless the client was given a default ttl
            of some kind.
        """
        await self._dbm.set(self.make_key(key), value, ttl)

    async def get(self, key: str) -> T | None:
        """Gets a single entry in the cache/databse.
        :param key: the key where the entry is."""
        return await self._dbm.get(self.make_key(key))

    async def get_iter(self) -> AsyncIterator[tuple[str, T]]:
        """Searches all keys by given name"""
        async for i in self._dbm.search_iter(self.namespace):
            yield i

    async def search_iter(self, key: str) -> AsyncIterator[tuple[str, T]]:
        """Searches all keys by given name"""
        async for k, v in self._dbm.search_iter(self.make_key(key)):
            yield k, v

    @abstractmethod
    async def search_all(self, key: str) -> dict[str, T]:
        """Searches all keys by given name into a dictionary"""
        return await self._dbm.search_all(self.make_key(key))

    async def get_all(self) -> dict[str, T]:
        """Gets all keys & values in the database"""
        return {k: v async for k, v in self.get_iter()}

    async def delete(self, key: str) -> None:
        """Deletes values by exact key name.

        :param key: the key to be deleted
        """
        return await self._dbm.delete(self.make_key(key))

    @abstractmethod
    async def expire_all(self) -> None:
        """Removes everything that has passed it's expiration date."""
        return await self._dbm.expire(self.namespace)

    async def incr(self, key: str) -> int | None:
        """
        increments entry's counter.
        :param key: the key to search for.
        :returns: the counter + 1 if key exists otherwise this is `None`
        """
        return await self._dbm.incr(self.make_key(key))

    async def decr(self, key: str) -> int | None:
        """
        decrements entry's counter.
        :param key: the key to search for.
        :returns: the counter - 1 if key exists and is not 0
            otherwise this is `None`
        """
        return await self._dbm.decr(key)

    async def expire(self, key: str | None = None) -> None:
        """Expries items based off key otherwise it expires everything"""
        if not key:
            await self.expire_all()
        else:
            await self._dbm.expire(self.make_key(key))

    def __getitem__(self, key: str) -> "NSDBM[T]":
        """Creates a namespace for a given item."""
        return self._next_ns(key)


class BaseDBM(ABC, Generic[T]):
    """Abstract DMB-like system for handling multiple database-like
    systems. It allows for great amounts of customizing also."""

    __slots__ = (
        "__weakref__",
        "_auto_expire",
        "_dec",
        "_default_ttl",
        "_delim",
        "_enc",
        "_kw",
    )

    def __init__(
        self,
        dec: AbstractDecoder[T] | None = None,
        enc: AbstractEncoder | None = None,
        default_ttl: float | timedelta | None = None,
        auto_expire: bool = True,
        delim: str = ".",
        **kw,
    ) -> None:
        self._dec: AbstractDecoder = dec or msgpack.Decoder()
        self._enc: AbstractEncoder = enc or msgpack.Encoder()
        self._default_ttl = default_ttl
        self._auto_expire = auto_expire
        self._delim = delim
        self._kw = kw

    def encode(self, item: Any) -> bytes:
        return self._enc.encode(item)

    def decode(self, item: Buffer) -> T:
        return self._dec.decode(item)

    def convert_ttl(self, ttl: float | timedelta | None) -> timedelta | None:
        if ttl is not None:
            return (
                timedelta(seconds=ttl)
                if not isinstance(ttl, timedelta)
                else ttl
            )

    def next_ttl(self, ttl: float | timedelta | None) -> datetime | None:
        """:returns: next ttl."""
        _ttl = self.convert_ttl(ttl) or self._default_ttl
        if _ttl is not None:
            return self.now() + _ttl

    def now(self) -> datetime:
        """Reports datetime the way you need it."""
        return datetime.now()

    async def close(self) -> None:
        """enables closing of the DBM-Like database"""
        if self._auto_expire:
            await self.expire()
        await self._close()

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self, *args):
        await self.close()

    @abstractmethod
    async def connect(self) -> None:
        """Connects to the database
        (it's meant to be used with `__aenter__`)"""
        pass

    @abstractmethod
    async def _close(self) -> None:
        pass

    @abstractmethod
    async def set(
        self,
        key: str,
        value: T,
        ttl: float | timedelta | None = None,
    ) -> None:
        """Sets a value to set or be updated.

        :param key: the key for the value.
        :param value: the value to set up.
        :param ttl: the expiration date otherwise it shall
            never expire unless the client was given a default ttl
            of some kind.
        """

    @abstractmethod
    async def get(self, key: str) -> T | None:
        """Gets a single entry in the cache/databse.
        :param key: the key where the entry is."""

    @abstractmethod
    async def get_iter(self) -> AsyncIterator[tuple[str, T]]:
        """Searches all keys by given name"""

    @abstractmethod
    async def search_iter(self, key: str) -> AsyncIterator[tuple[str, T]]:
        """Searches all keys by given name"""

    async def search_all(self, key: str) -> dict[str, T]:
        """Searches all keys by given name into a dictionary"""
        return {k: v async for k, v in self.search_iter(key)}

    async def get_all(self) -> dict[str, T]:
        """Gets all keys & values in the database"""
        return {k: v async for k, v in self.get_iter()}

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Deletes values by exact key name.

        :param key: the key to be deleted
        """

    @abstractmethod
    async def expire_all(self) -> None:
        """Removes everything that has passed it's expiration date."""

    # Inspired by aiomache

    @abstractmethod
    async def incr(self, key: str) -> int | None:
        """
        increments entry's counter.
        :param key: the key to search for.
        :returns: the counter + 1 if key exists otherwise this is `None`
        """

    @abstractmethod
    async def decr(self, key: str) -> int | None:
        """
        decrements entry's counter.
        :param key: the key to search for.
        :returns: the counter - 1 if key exists and is not 0
            otherwise this is `None`
        """
        pass

    @abstractmethod
    async def expire(self, key: str | None = None) -> None:
        """Expries items based off key otherwise it expires everything"""
        pass

    def __getitem__(self, key: str) -> NSDBM[T]:
        """Creates a namespace for a given item."""
        return NSDBM(self, [key])

    def wrap(
        self, namespace: str | None = None
    ) -> Callable[
        [Callable[Concatenate[Self | NSDBM[T], P], Coroutine[Any, Any, R]]],
        Callable[P, Coroutine[Any, Any, R]],
    ]:
        """Wraps connections to a function to make handling a function easier
        and avoiding duplicate async withs."""

        def decorator(
            func: Callable[
                Concatenate[Self | NSDBM[T], P], Coroutine[Any, Any, R]
            ],
        ) -> Callable[P, Coroutine[Any, Any, T]]:
            @wraps(func)
            async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                async with self:
                    if namespace:
                        return await func(self[namespace], *args, **kwargs)
                    else:
                        return await func(self, *args, **kwargs)

            return wrapper

        return decorator
