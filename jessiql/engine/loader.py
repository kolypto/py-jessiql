""" Loading strategies for Query """

from __future__ import annotations

from collections import abc

import sqlalchemy as sa

from jessiql.query_object import SelectedRelation
from jessiql.typing import SAModelOrAlias, SARowDict
from .jselectinloader import JSelectInLoader


class QueryLoaderBase:
    def prepare_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        return stmt

    def for_states(self, source_states: list[SARowDict]):
        pass

    def load_results(self, stmt: sa.sql.Select, connection: sa.engine.Connection) -> abc.Iterator[SARowDict]:
        raise NotImplementedError


class PrimaryQueryLoader(QueryLoaderBase):
    __slots__ = ()

    def load_results(self, stmt: sa.sql.Select, connection: sa.engine.Connection) -> abc.Iterator[SARowDict]:
        # Get the result, convert list[RowMapping] into list[dict]
        res: sa.engine.CursorResult = connection.execute(stmt)
        # TODO: use fetchmany() or partitions()
        #   See how jessiql behaves with huge result sets. Make sure it's able to iterate, not load everything into memory.
        #   See: https://docs.sqlalchemy.org/en/14/_modules/examples/performance/large_resultsets.html
        yield from (dict(row) for row in res.mappings())


class RelatedQueryLoader(QueryLoaderBase):
    __slots__ = 'relation', 'loader'

    def __init__(self, relation: SelectedRelation, source_Model: SAModelOrAlias, target_Model: SAModelOrAlias):
        self.relation = relation
        self.loader = JSelectInLoader(source_Model, self.relation.property, target_Model)

    def for_states(self, source_states: list[SARowDict]):
        self.loader.prepare_states(source_states)

    def prepare_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        return self.loader.prepare_query(stmt)

    def load_results(self, stmt: sa.sql.Select, connection: sa.engine.Connection) -> abc.Iterator[SARowDict]:
        yield from self.loader.fetch_results_and_populate_states(connection, stmt)
