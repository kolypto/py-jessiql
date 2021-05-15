import pytest
import sqlalchemy as sa

from . import globals


@pytest.fixture(scope='function')
# TODO: parametrize() with future=True and future=False
def engine() -> sa.engine.Engine:
    return globals.engine


@pytest.fixture(scope='function')
def connection(engine: sa.engine.Engine) -> sa.engine.Connection:
    with engine.connect() as conn:
        yield conn
