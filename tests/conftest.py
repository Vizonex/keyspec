import sys
from importlib.util import find_spec
from typing import Any

import pytest

from keyspec.base import BaseDBM


def has_module(library: str) -> bool:
    """
    Finds out if library exists without executing any code for the library.
    """
    return find_spec(library) is not None


PARAMS = [pytest.param(("asyncio", {"use_uvloop": False}), id="asyncio")]

# NOTE: Extensions are optional now...
if has_module("winloop" if sys.platform == "win32" else "uvloop"):
    PARAMS.append(
        pytest.param(("asyncio", {"use_uvloop": True}), id="asyncio+uvloop")
    )

if has_module("trio"):
    PARAMS.append(
        pytest.param(
            ("trio", {"restrict_keyboard_interrupt_to_checkpoints": True}),
            id="trio",
        )
    )

DB_PARAMS: list[type[BaseDBM]] = []

if has_module("anyio_cysqlite"):
    from keyspec.cysqlite import Database

    DB_PARAMS.append(pytest.param(Database, id="cysqlite"))


if has_module("sqlite_anyio"):
    from keyspec.sqlite import Database

    DB_PARAMS.append(pytest.param(Database, id="sqlite3"))


@pytest.fixture(params=PARAMS)
def anyio_backend(request: pytest.FixtureRequest) -> Any:
    return request.param


@pytest.fixture(params=DB_PARAMS)
async def dbm(request: pytest.FixtureRequest) -> type[BaseDBM]:
    return request.param
