""" Loading strategies for Query

* `QueryLoaderBase`: base class
* `PrimaryQueryLoader` for the primary query (top level)
* `RelatedQueryLoader` for related queries (joined queries)
"""

from __future__ import annotations

from collections import abc

import sqlalchemy as sa

from jessiql.query_object import SelectedRelation
from jessiql.typing import SAModelOrAlias, SARowDict

from .jselectinloader import JSelectInLoader


class QueryLoaderBase:
    """ Loader base

    Base for classes that implement:
    * Prepare an SQL statement for loading objects
    * Execute this statement
    * Populate existing objects with loaded related fields (applicable to related loaders)

    Operations, such as select, filter, sort, skip/limit, are out of scope here.
    """

    def prepare_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        """ Hook: prepare the SELECT statement before any operation is applied

        Use it to add columns & conditions that the loader will need.

        NOTE: called before any operation had a chance to modify the statement.

        Args:
             stmt: A boilerplate SELECT statement against the current model
        """
        return stmt

    def for_states(self, source_states: list[SARowDict]):
        """ Associate the loader with a list of "states": objects loaded by the parent loader

        Only makes sense for related loaders
        """

    def load_results(self, stmt: sa.sql.Select, connection: sa.engine.Connection) -> abc.Iterator[SARowDict]:
        """ Actually execute the query and handle result rows fetched from it.

        NOTE: called when all operations have already been applied to the statement.
        NOTE: for_states() has already been called.

        Args:
            stmt: The statement to execute
            connection: The connection to execute the statement with

        Returns:
            Iterator of result dicts
        """
        raise NotImplementedError


class PrimaryQueryLoader(QueryLoaderBase):
    """ Primary loader: for the top-level model

    This loader is used for the primary model: the one at the top.
    """
    __slots__ = ()

    def load_results(self, stmt: sa.sql.Select, connection: sa.engine.Connection) -> abc.Iterator[SARowDict]:
        # TODO: use fetchmany() or partitions()
        #   See how jessiql behaves with huge result sets. Make sure it's able to iterate, not load everything into memory.
        #   See: https://docs.sqlalchemy.org/en/14/_modules/examples/performance/large_resultsets.html

        # Get the result
        # We use `.mappings()` to convert a list of rows `list[RowMapping]` into a list of dicts `list[dict]`
        res: sa.engine.CursorResult = connection.execute(stmt)
        yield from (dict(row) for row in res.mappings())


class RelatedQueryLoader(QueryLoaderBase):
    """ Related loader: for related models

    This loader is used to populate loaded models with related fields.
    """
    __slots__ = 'loader',

    def __init__(self, relation: SelectedRelation, source_Model: SAModelOrAlias, target_Model: SAModelOrAlias):
        # Relies on `JSelectInLoader`: implementation borrowed from SqlAlchemy's SelectInLoader
        self.loader = JSelectInLoader(source_Model, relation.property, target_Model)

    def for_states(self, source_states: list[SARowDict]):
        # The list of states this loader is going to populate with related fields
        # Pass it to JSelectInLoader
        self.loader.prepare_states(source_states)

    def prepare_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        # Prepare the statement.
        # Pass it to JSelectInLoader
        return self.loader.prepare_query(stmt)

    def load_results(self, stmt: sa.sql.Select, connection: sa.engine.Connection) -> abc.Iterator[SARowDict]:
        # Use JSelectInLoader to fetch results and populate existing states with related fields
        yield from self.loader.fetch_results_and_populate_states(connection, stmt)
