""" Query: an object that binds operations together """

from __future__ import annotations

from collections import abc
from typing import Union

import sqlalchemy as sa

from jessiql import operations
from jessiql.query_object import QueryObject, SelectedRelation
from jessiql.sainfo.models import unaliased_class
from jessiql.typing import SAModelOrAlias, SAAttribute, SARowDict

from .loader import QueryLoaderBase, PrimaryQueryLoader, RelatedQueryLoader


# The type for load paths
# Example:
#   (User,)
#   (User, 'articles', Article)
#   (User, 'articles', Article, 'comments', Comment)
from ..query_object.resolve import resolve_query_object

LoadPath = tuple[Union[type, SAAttribute]]


# A callable that customizes a statement
CustomizeStatementCallable = abc.Callable[[sa.sql.Select, LoadPath], sa.sql.Select]

# A callable that
CustomizeResultsCallable = abc.Callable[[list[SARowDict], LoadPath], list[SARowDict]]


class QueryExecutor:
    query: QueryObject
    target_Model: SAModelOrAlias

    customize_statements: list[CustomizeStatementCallable]
    customize_results: list[CustomizeResultsCallable]

    loader: QueryLoaderBase
    load_path: operations.LoadPath
    related_executors: dict[str, QueryExecutor] = None

    def __init__(self, query: QueryObject, target_Model: SAModelOrAlias):
        self.query = query
        self.target_Model = target_Model
        self.load_path = (unaliased_class(target_Model),)

        self.customize_statements = []
        self.customize_results = []

        # Resolve every input
        resolve_query_object(self.query, self.target_Model)

        # Init operations
        self.select_op = self.SelectOperation(query, target_Model)
        self.filter_op = self.FilterOperation(query, target_Model)
        self.sort_op = self.SortOperation(query, target_Model)
        self.skiplimit_op = self.SkipLimitOperation(query, target_Model)

        # Init loader
        self.loader = self.PrimaryQueryLoader()

    def for_relation(self, source_executor: QueryExecutor, relation: SelectedRelation, source_states: list[SARowDict]):
        self.load_path = source_executor.load_path + (relation.name, unaliased_class(self.target_Model))

        source_Model = source_executor.target_Model
        self.loader = self.RelatedQueryLoader(relation, source_Model, self.target_Model, source_states)

        self.skiplimit_op.paginate_over_foreign_keys(relation.property.remote_side)

        return self

    def fetchall(self, connection: sa.engine.Connection) -> list[SARowDict]:
        # Load all results
        states = list(self._load_results(connection))

        # Load all relations
        self._load_relations(connection, states)

        # Customize
        for handler in self.customize_results:
            states = handler(states, self.load_path)

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
                source_executor=self,
                relation=relation,
                source_states=states
            )
            executor.customize_statements = self.customize_statements
            executor.customize_results = self.customize_results

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

        for handler in self.customize_statements:
            stmt = handler(stmt, self.load_path)

        # This one has to be applied last because it may wrap the query into a subquery
        stmt = self.skiplimit_op.apply_to_statement(stmt)

        return stmt
