from typing import Union

import sqlalchemy as sa


def insert(connection: sa.engine.Connection, Model: Union[sa.sql.Selectable, type], *values):
    """ Helper: run a query to insert many rows of Model into a table using low-level SQL statement

    Example:
        insert(connection, Model, [
            dict(id=1),
            dict(id=2),
            dict(id=3),
        ])
    """
    stmt = sa.insert(Model).values(*values)
    connection.execute(stmt)
