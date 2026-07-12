# keyspec
[![PyPI version](https://badge.fury.io/py/keyspec.svg)](https://badge.fury.io/py/keyspec)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/keyspec)](https://pypi.org/project/keyspec)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache-yellow.svg)](https://opensource.org/licenses/Apache-2-0)

A msgspec-like database system for python inspired by aiocache.

## Installation
Currently there are 2 different extensions to use those being `cysqlite` and `sqlite` backends.
More backends are being planned in a future release such as `lmdb`.

## Cysqlite implementation
```
pip install keyspec[cysqlite]
```

## Sqlite implementation
```
pip install keyspec[sqlite]
```

## LMDB implementation
Coming soon...


# Common Usage
Using the database system and finding the implementation you want should be very easy to figure out.
in this example below we use `cysqlite` which has slightly better performance than `sqlite` the
sqlite version can be utilized for support with the standard python libraries if dependencies are 
more of a concern to your needs.
```python
import anyio
from msgspec import Struct, msgpack

from keyspec.cysqlite import Database


class User(Struct):
    name: str
    password: str

# you can specify either a type or a msgspec decoder of your choice.
database = Database("my_database.db", User)
...
# Alternative approch if custom decoders are needed.
database = Database("my_database.db", dec=msgpack.Decoder(User))


# you can wrap a database as needed or using async with
@database()
async def insert(user_db: Database[User], username:str, password: str) -> None:
    await user_db.set("user", User(username, password))
    return await user_db.get_all()

async def async_with_insert(username:str, password: str) -> None:
    async with  Database("my_database.db", User) as user_db:
        await user_db.set("user", User(username, password))
        return await user_db.get_all()

async def async_with_insert_alternative_method(username:str, password: str) -> None:
    async with database as user_db:
        await user_db.set("user", User(username, password))
        return await user_db.get_all()

@database()
async def namespaces(user_db: Database[User]) -> User | None:
    # namespaces can also be set just like how it's done in redis.
    await user_db["namespace"].set("user", User("user", "pass"))
    return await user_db["namespace"].get("user")


@database()
async def get_all(user_db: Database[User]) -> dict[str, User]:
    return await user_db.get_all()

async def test():
    # NOTE: Client is deleted as the wrapper cane take care of
    # the first attribute for you making the database wrapper 
    # an easy to use shortcut to access your newly made database.
    await insert("user", "pass")
    await insert("user1", "password")
    data = await get_all()
    # {
    #   'user': User(name='user', password='pass'),
    #   'user1': User(name='user1', password='password')
    # }
    print(data)

if __name__ == "__main__":
    anyio.run(test)

```

## Expirable Data
Because sometimes we just want to implement a cache system
and recycle the unused data later.
A Good example is preventing bots from just mixing in with 
our users.

```python
from msgspec import Struct
from keyspec.sqlite import Database
from datetime import timedelta

class Captcha(Struct):
    answer: str

# Time to Live and expirable systems are also accepted
captcha_db = Database("captchas.db", Captcha, default_ttl=60) # 60 seconds
captcha_db = Database("captchas.db", Captcha, default_ttl=timedelta(hours=1)) # 1 hour
# Data automatically will expire upon exiting the
# function or context manager being entered otherwise you can use auto_expire=False to set
# your own rules on how data should be expired
captcha_db = Database(
    "captchas.db",
    Captcha, default_ttl=timedelta(hours=1), auto_expire=True
)
```


## Why I wrote it.
-   Msgspec doesn't work as an __sqlalchemy__ table on it's own 
    and therefore there can't be any real competitor with __SQLModel__ and why write a class object twice, it's unnessesary. 
    the reason for this is that __sqlalchemy__ can't make use of `__slots__`. This was an alternative approch in the longrun that 
    at least gives msgspec some form of database functionality.

-   Keyspec is smaller than __sqlalchemy__ and is made in a redis-like format. Know however there is a disadvantage as
    only single key lookups can be done, so choose your keys wisely, the best choice by far is the name or id of 
    the object you wish to serlize and deserlize out of your database.

-   Keyspec gives the user a lot of creativity right out of the box.

-   Windows doesn't have a saveable cache system with redis as far as I am aware
    so I gave it cysqlite and anyio to at least make up for that. 
    As someone who hates OS-Gating, giving a proper database system for windows/linux and Apple devices was a priority for me.

-   I was requested by the aio-libs team that if there was another cache extension 
    library, it would have to be put somewhere else. So I ended up just making my own cache-like
    database with a better system to utilize. It has extensions if you want to build your own database.

-   Anyio is a better choice as you automatically can support both `trio` and `asyncio` and any server implementation under the sun,
    however __aiohttp__ may require a bit of setup as you need to serlize the data to the format the client may be requesting for 
    and __aiohttp__ doesn't support trio unless you use the trio eventloop library.

-   You can automatically pair litestar with keyspec with the best results however `fastapi`
    may require a bit of work to get data to be streamed correctly.

-   Compared to aiocache the `set` function adds or updates rather than one or the other. 
    This just gets rid of another annoyance I had all together.

-   It doesn't take much practice at all to get used to using the library even a less skilled
    programmer could pick up this library with a little bit of paitience.

-   Cysqlite extension is regularly updated and maintained alongside it's companion library __anyio-cysqlite__ which is written by me, 
    the same author of this library.


