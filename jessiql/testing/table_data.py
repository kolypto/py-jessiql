from itertools import chain
from typing import Union

import sqlalchemy as sa

from jessiql.sainfo.primary_key import primary_key_columns


def insert(connection: sa.engine.Connection, Model: Union[sa.sql.Selectable, type], *values: dict):
    """ Helper: run a query to insert many rows of Model into a table using low-level SQL statement

    Example:
        insert(connection, Model,
               dict(id=1),
               dict(id=2),
               dict(id=3),
        )
    """
    all_keys = set(chain.from_iterable(d.keys() for d in values))
    assert values[0].keys() == set(all_keys), 'The first dict() must contain all possible keys'

    stmt = sa.insert(Model).values(values)
    connection.execute(stmt)


def loadall(ssn: sa.orm.Session, Model: type):
    """ Load all rows from a table, ordered by primary key """
    pk_cols = primary_key_columns(Model)
    return ssn.query(Model).order_by(*pk_cols)
