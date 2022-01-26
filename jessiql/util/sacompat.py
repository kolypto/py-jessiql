import sqlalchemy as sa
from collections import abc

from jessiql.sainfo.version import *  # noqa: shortcut


try:
    # SA 1.4
    from sqlalchemy.orm import declarative_base, DeclarativeMeta
except ImportError:
    # 1.3
    from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta


def add_columns_if_missing(stmt: sa.sql.Select, columns: abc.Iterable[sa.Column]) -> sa.sql.Select:
    """ Add columns to an SQL Select statement, but only if they're not already added """

    # NOTE: in SqlAlchemy 1.4.23 add_columns() does not do de-duplication anymore.
    # If this breaks your code like it broke jessiql, use this function

    if SA_13:
        new_columns = (col for col in columns if not stmt.columns.contains_column(col))
    else:
        new_columns = (col for col in columns if not stmt.selected_columns.contains_column(col))

    return add_columns(stmt, new_columns)


def add_columns(stmt: sa.sql.Select, columns: abc.Iterable[sa.Column]) -> sa.sql.Select:
    """ Add columns to an SQL Select statement """
    if SA_13:
        for col in columns:
            stmt.append_column(col)
    else:
        stmt = stmt.add_columns(*columns)

    return stmt
