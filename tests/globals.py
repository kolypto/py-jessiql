import os
from collections import abc

import sqlalchemy as sa
import sqlalchemy.engine
import sqlalchemy.orm


# URL of the database to connect to
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://postgres:postgres@localhost/test_jessiql')


engine: sa.engine.Engine = sa.engine.create_engine(
    DATABASE_URL,
    future=True,  # SA 1.4: 2.0 forward compatibility
    executemany_mode='batch',
)


SessionMakerCallable = abc.Callable[[], sa.orm.Session]


SessionMaker: SessionMakerCallable = sa.orm.sessionmaker(
    future=True,  # SA 1.4: 2.0 forward compatibility
    autocommit=False,
    autoflush=False,
    bind=engine,
)

