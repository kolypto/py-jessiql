""" Query: an object that binds operations together """

from __future__ import annotations

import sqlalchemy as sa

from collections import abc

from jessiql.query_object import QueryObject, SelectedRelation
from jessiql.typing import SAModelOrAlias, SARowDict
from jessiql import operations

from .loader import QueryLoaderBase, PrimaryQueryLoader, RelatedQueryLoader


class QueryExecutor:
    query: QueryObject
    target_Model: SAModelOrAlias
    loader: QueryLoaderBase

    def __init__(self, query: QueryObject, target_Model: SAModelOrAlias):
        self.query = query
        self.target_Model = target_Model

        # Init operations
        self.select_op = self.SelectOperation(query, target_Model)
        self.filter_op = self.FilterOperation(query, target_Model)
        self.sort_op = self.SortOperation(query, target_Model)
        self.skiplimit_op = self.SkipLimitOperation(query, target_Model)

        # Init loader
        self.loader = self.PrimaryQueryLoader()

    related_executors: dict[str, QueryExecutor] = None

    def for_relation(self, relation: SelectedRelation, source_Model: SAModelOrAlias, source_states: list[SARowDict]):
        self.loader = self.RelatedQueryLoader(relation, source_Model, self.target_Model, source_states)
        self.skiplimit_op.paginate_over_foreign_keys(relation.property.remote_side)
        return self

    def fetchall(self, connection: sa.engine.Connection) -> list[SARowDict]:
        # Load all results
        states = list(self._load_results(connection))

        # Load all relations
        self._load_relations(connection, states)

        # Done
        return states

    PrimaryQueryLoader = PrimaryQueryLoader
    RelatedQueryLoader = RelatedQueryLoader

    SelectOperation = operations.SelectOperation
    FilterOperation = operations.FilterOperation
    SortOperation = operations.SortOperation
    SkipLimitOperation = operations.SkipLimitOperation

    def _load_relations(self, connection: sa.engine.Connection, states: list[SARowDict]):
        """ Load every relation's rows """
        QueryExecutor = self.__class__
        self.related_executors = {}

        for relation in self.query.select.relations.values():
            target_Model = relation.property.mapper.class_

            self.related_executors[relation.name] = executor = QueryExecutor(relation.query, target_Model).for_relation(
                relation=relation,
                source_Model=self.target_Model,
                source_states=states
            )
            executor.fetchall(connection)

    def _load_results(self, connection: sa.engine.Connection) -> abc.Iterator[SARowDict]:
        stmt = self._statement()
        yield from self.loader.load_results(stmt, connection)

    def _statement(self) -> sa.sql.Select:
        stmt = sa.select([]).select_from(self.target_Model)

        stmt = self._apply_operations_to_statement(stmt)

        return stmt

    def _apply_operations_to_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        stmt = self.loader.prepare_statement(stmt)

        stmt = self.select_op.apply_to_statement(stmt)
        stmt = self.filter_op.apply_to_statement(stmt)
        stmt = self.sort_op.apply_to_statement(stmt)
        stmt = self.skiplimit_op.apply_to_statement(stmt)
        return stmt
