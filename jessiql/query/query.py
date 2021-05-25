""" Query: an object that binds operations together """

import sqlalchemy as sa
import sqlalchemy.orm

from collections import abc

from jessiql.query_object import QueryObject, SelectedRelation
from jessiql.typing import SAModelOrAlias, SARowDict
from jessiql import operations

from .jselectinloader import JSelectInLoader


class Query:
    def __init__(self, query: QueryObject, target_Model: SAModelOrAlias):
        self.query = query
        self.target_Model = target_Model

        # Init operations
        self.select_op = self.SelectOperation(query, target_Model)
        self.filter_op = self.FilterOperation(query, target_Model)
        self.sort_op = self.SortOperation(query, target_Model)
        self.skiplimit_op = self.SkipLimitOperation(query, target_Model)

        # Init every relation
        self.relations = {
            relation.name: JoinedQuery(query, ...)
            for relation in query.select.relations.values()
        }

    SelectOperation = operations.SelectOperation
    FilterOperation = operations.FilterOperation
    SortOperation = operations.SortOperation
    SkipLimitOperation = operations.SkipLimitOperation
    JoinedQuery = JoinedQuery

    def all(self, connection: sa.engine.Connection) -> list[SARowDict]:
        # Load all results
        states = list(self._load_results(connection))

        #

        # Done
        return states

    def _load_results(self, connection: sa.engine.Connection) -> abc.Iterator[SARowDict]:
        # Get the result, convert list[RowMapping] into list[dict]
        stmt = self._statement()
        res: sa.engine.CursorResult = connection.execute(stmt)
        yield from (dict(row) for row in res.mappings())  # TODO: use fetchmany() or partitions()

    def _statement(self) -> sa.sql.Statement:
        stmt = sa.select([]).select_from(self.target_Model)

        stmt = self._apply_operations_to_statement(stmt)

        return stmt

    def _apply_operations_to_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        stmt = self.select_op.apply_to_statement(stmt)
        stmt = self.filter_op.apply_to_statement(stmt)
        stmt = self.sort_op.apply_to_statement(stmt)
        stmt = self.skiplimit_op.apply_to_statement(stmt)
        return stmt


class JoinedQuery(Query):
    source_Model: SAModelOrAlias
    relation: SelectedRelation
    loader: JSelectInLoader

    def _load_results(self, connection: sa.engine.Connection) -> abc.Iterator[SARowDict]:
        stmt = self._statement()
        yield from self.loader.fetch_results_and_populate_states(connection, stmt)

    def for_relation(self, relation: SelectedRelation, source_Model: SAModelOrAlias, source_states: list[SARowDict]):
        self.relation = relation
        self.loader = JSelectInLoader(source_Model, self.relation.property, self.target_Model)
        self.loader.prepare_states(source_states)

        self.skiplimit_op.paginate_over_foreign_keys(self.relation.property.remote_side)

        return self

    def _apply_operations_to_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        stmt = self.loader.prepare_query(stmt)
        stmt = super()._apply_operations_to_statement(stmt)
        return stmt
