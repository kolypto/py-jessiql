import os
from collections import abc

import sqlalchemy as sa
import sqlalchemy.orm

from jessiql.sainfo.version import SA_13, SA_14


# URL of the database to connect to
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://postgres:postgres@localhost/test_jessiql')


engine: sa.engine.Engine = sa.engine.create_engine(
    DATABASE_URL,
    executemany_mode='batch',
    **({
        SA_13: dict(),
        SA_14: dict(
            future=True,  # SA 1.4: 2.0 forward compatibility
        ),
    }[True])
)


SessionMakerCallable = abc.Callable[[], sa.orm.Session]


SessionMaker: SessionMakerCallable = sa.orm.sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    **({
        SA_13: dict(),
        SA_14: dict(
            future=True,  # SA 1.4: 2.0 forward compatibility
        ),
    }[True])
)

