import os
import pytest
import sqlalchemy as sa

from jessiql.sainfo.version import SA_13, SA_14


@pytest.fixture(scope='function', params=[
    # Parameter: "future=True/False"
    pytest.param(
        (True,),
        marks=pytest.mark.skipif(not SA_14, reason="future=True makes no sense for SA 1.3. Won't waste time on that.")
    ),
    (False,),
])
def engine(request) -> sa.engine.Engine:
    # `future=` arg
    future, = request.param
    if SA_13:
        kwargs = dict()
    elif SA_14:
        kwargs = dict(future=future)
    else:
        raise NotImplemented

    # Engine
    return sa.engine.create_engine(
        DATABASE_URL,
        executemany_mode='batch',
        **kwargs
    )


@pytest.fixture(scope='function')
def connection(engine: sa.engine.Engine) -> sa.engine.Connection:
    with engine.connect() as conn:
        yield conn


# URL of the database to connect to
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://postgres:postgres@localhost/test_jessiql')


# SessionMaker: SessionMakerCallable = sa.orm.sessionmaker(
#     bind=engine,
#     autocommit=False,
#     autoflush=False,
#     **({
#         SA_13: dict(),
#         SA_14: dict(
#             future=True,  # SA 1.4: 2.0 forward compatibility
#         ),
#     }[True])
# )
