# keyspec
A msgspec-like database system for python inspired by aiocache.

## Common Usage
```python
from msgspec import Struct

from keyspec import Client, cache

# Create your msgspec Structure, Attrs dataclass or etc...
class User(Struct):
    name: str
    password: str

# you can get pretty lazy if needed.
factory = cache(
    "data.db",
    User,
)


@factory
async def instert(client: Client[User], username: str, password: str) -> None:
    await client.set(username, User(username, password))

# The less lazy option is to do this.
@cache(
    "data.db",
    User,
)
async def get(client: Client[User], username: str) -> User | None:
    return await client.get(username)


@factory
async def get_all(client: Client[User]) -> dict[str, User]:
    return await client.get_all()


async def test():
    # NOTE: Client is deleted as the factory takes care of
    # the first attribute for you...
    await instert("user", "pass")
    await instert("user1", "password")
    data = await get_all()
    # {
    #   'user': User(name='user', password='pass'),
    #   'user1': User(name='user1', password='password')
    # }
    print(data)


if __name__ == "__main__":
    import anyio

    anyio.run(test)
```

## Why I wrote it.
- Msgspec doesn't work as a sqlalchemy table on it's own 
and therefore there can't be any real competitor with SQLModel and why write a class object twice, it's unnessesary. the reason for this is that sqlalchemy can't make use of `__slots__`. 
This was an alternative approch in the longrun that at least gives msgspec some form of database functionality.

- Windows doesn't have a saveable cache system with redis as far as I am aware
so I gave it cysqlite and anyio to at least make up for that. 
As someone who hates OS-Gating, giving a proper database system for windows/linux and Apple devices was a priority for me.

- I was requested by the aio-libs team that if there was another cache extension 
library it would have to be put somewhere else. So I ended up just making my own cache-like
database with a better system to utilize. This one uses msgpack to help with the speed of 
decoding the objects however, I may add a feature that allows for json to be used instead. (This may lead to a new subclassable wrapper system and a new cache wrapper).

- Compared to aiocache the `set` function adds or updates rather than one or the other. 
This just gets rid of another annoyance I had all together.

- It doesn't take much practice at all to get used to using the library, even a beginner 
with a bit of knowlege on how anyio works could figure this out just fine. If you need 
something quick, dirty, or lazy, look no further than the `cache` function provided just for you that automatically opens and closes the database with the use of a single async function wrapper, you can even set the cache as a variable to wrap. For example: a factory attribute to save a bit of precious time and productivity (This was the ultimate goal after all...).

- You get anyio support which means it will run on trio and asyncio eventloops.

- Cysqlite is regularly updated and maintained alongside it's companion library anyio-cysqlite which is written by me, the same author of this library.

- With anyio support, it is possible to use this library with litestar which has support for msgsepc by default. Examples may include captchas (with the use of a decently timed ttl) or user accounts by given name (recommended). TOTP tools for 2fa logins might also be a good one. The possibilities are endless.


