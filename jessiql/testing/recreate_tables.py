""" Recreate DB structure -- for testing"""

from __future__ import annotations

from collections import abc
from contextlib import contextmanager
from typing import Union

from sqlalchemy import MetaData

import sqlalchemy as sa

from jessiql.util import sacompat


@contextmanager
def created_tables(bind: EngineOrConnection, metadata: MetaData):
    """ Temporarily create tables, drop them when the context is quit

    Example:
        Base = sa.orm.declarative_base()
        conn = engine.connect()

        with created_tables(conn, Base.metadata):
            ...

    Args:
        bind: A connectable: Engine oe Connection
    """
    metadata = get_metadata(metadata)

    create_tables(bind, metadata)
    try:
        yield
    finally:
        drop_tables(bind, metadata)


def truncate_or_recreate_db_tables(bind: EngineOrConnection, metadata: MetaData):
    """ Prepare a clean database: TRUNCATE tables if possible, DROP/CREATE

    This function attempts to provide you with a clean database as fast as possible:
    it's because TRUNCATE is fast; DROP/CREATE is not.

    Args:
        bind: A connectable: Engine oe Connection
    """
    metadata = get_metadata(metadata)

    if check_recreate_necessary(bind, metadata):
        recreate_db_tables(bind, metadata)
    else:
        truncate_db_tables(bind, metadata)


def recreate_db_tables(bind: EngineOrConnection, metadata: MetaData):
    """ Prepare a clean database: drop all tables, then create them.

    This is an expensive operation, but provides the cleanest results:
    it is guaranteed that the structure matches the one described in your code.
    """
    metadata.drop_all(
        bind=bind,
        tables=list(metadata.tables.values())
    )
    metadata.create_all(bind=bind)


def truncate_db_tables(bind: EngineOrConnection, metadata: MetaData):
    """ Prepare a clean database: truncate all tables

    This is a cheaper operation, but it requires that tables in your code have the same structure.

    Args:
        bind: A connectable: Engine oe Connection
    """
    table_names = [table.name
                   for table in reversed(metadata.sorted_tables)]
    truncate_query = f"TRUNCATE {','.join(table_names)} RESTART IDENTITY CASCADE;"
    bind.execute(truncate_query)


def create_tables(bind: EngineOrConnection, metadata: MetaData):
    """ CREATE tables in the provided metadata """
    metadata.create_all(bind=bind)


def drop_tables(bind: EngineOrConnection, metadata: MetaData):
    """ DROP tables in the provided metadata """
    metadata.drop_all(
        bind=bind,
        tables=list(metadata.tables.values())
    )


def drop_existing_tables(bind: EngineOrConnection):
    """ List existing tables in the database and drop them """
    # Reflect
    reflected_metadata = MetaData(bind=bind)
    reflected_metadata.reflect()

    # Drop
    reflected_metadata.drop_all(bind=bind)


def check_recreate_necessary(bind: EngineOrConnection, metadata: MetaData) -> bool:
    """ See if if it's necessary to recreate tables?

    List all existing table columns, compare them with the models.
    Tell if there's been any modifications.

    This function is used in unit-test runs to save some time recreating the database when it's not necessary.

    Example:
        metadata = Base.metadata
        conn = engine.connect()

        if check_recreate_necessary(conn, metadata):
            recreate_db_tables(conn, metadata)
        else:
            truncate_db_tables(conn, metadata)
    """
    # Actual metadata
    reflected_metadata = MetaData(bind=bind)
    reflected_metadata.reflect()

    # Compare
    expected_table_columns = _get_table_column_pairs(metadata)
    actual_table_columns = _get_table_column_pairs(reflected_metadata)

    # Not equal? Need to recreate
    return set(expected_table_columns) != set(actual_table_columns)


def get_metadata(obj: Union[MetaData, type]) -> MetaData:
    """ Get metadata (DB structure) from an object """
    # Declarative class
    if isinstance(obj, sacompat.DeclarativeMeta):  # type: ignore[attr-defined]  # sqlalchemy moved the definition but stubs are not up to date
        return obj.metadata  # type: ignore[union-attr]  # it things that `type` has no metadata
    # MetaData object
    elif isinstance(obj, MetaData):
        return obj
    # Unsupported
    else:
        raise NotImplementedError


def _get_table_column_pairs(metadata: MetaData) -> abc.Iterable[tuple[str, str]]:
    """ Iterate metadata and yield (table_name, column_name) pairs

    This function is used to get (table,column) pairs from an existing database, or python-defined tables.
    """
    for table_name, table in metadata.tables.items():
        for column_name, column in table.columns.items():
            yield table_name, column_name


EngineOrConnection = Union[sa.engine.Engine, sa.engine.Connection]
