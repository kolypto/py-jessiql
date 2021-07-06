""" QueryExecutor: an object that binds operations together to execute a Query Object

This is low level. See Query.
"""

from __future__ import annotations

from collections import abc
from typing import Union

import sqlalchemy as sa

from jessiql import operations
from jessiql.query_object import QueryObject, SelectedRelation
from jessiql.query_object.resolve import resolve_query_object
from jessiql.sainfo.models import unaliased_class
from jessiql.typing import SAModelOrAlias, SARowDict

from .loader import QueryLoaderBase, PrimaryQueryLoader, RelatedQueryLoader


class QueryExecutor:
    """ Query Executor: executes operations on a Query Object

    This low-level class puts everything together and applies individual operations to a Query Object.
    It initiates an SqlAlchemy Core Statement (`sa.Select`), lets every operation modify it (select, filter, sort, skiplimit),
    then uses a Loader object to fetch actual rows.

    This is a low-level interface.
    See `Query`
    """
    # The Query Object to execute.
    query: QueryObject

    # The target model to execute the Query against
    Model: SAModelOrAlias

    # Load path: (Model, attribute, ...) path to the current model
    # Examples:
    # > (User,)
    # > (User, 'articles', Article)
    # > (User, 'articles', Article, 'comments', Comment)
    # Use it in your customization handler to know where you are.
    load_path: LoadPath

    # Query customization handler.
    # This is your last chance to make changes to the query.
    # Modifiable.
    # Example usage: provide additional filtering, e.g. for security
    # Example usage:
    #   @query.customize_statements.append
    #   def security_filter(query: QueryExecutor, statement: sa.sql.Select)
    # Arguments: (self, statement)
    customize_statements: list[CustomizeStatementCallable]

    # Results customization handler.
    # This is your last chance to catch results from every individual query.
    # Modifiable.
    # Example usage: pre-process loaded related objects
    # Arguments: (self, statement)
    customize_results: list[CustomizeResultsCallable]

    # Loader used to fetch results
    # Typically, a PrimaryQueryLoader or a RelatedQueryLoader
    # Is replaced when for_relation() is used
    loader: QueryLoaderBase

    # Child executors for related objects
    related_executors: dict[str, QueryExecutor] = None

    def __init__(self, query: QueryObject, Model: SAModelOrAlias):
        """ Initialize a Query Executor for the given Query Object

        Args:
            query: The Query Object to execute
            Model: The SqlAlchemt Model class to execute the query against
        """
        # The query and the model to query against
        assert isinstance(query, QueryObject)
        self.query = query
        self.Model = Model

        # Load path
        # May be modified by for_relation()
        self.load_path = (unaliased_class(Model),)

        # Customization handlers
        # May be modified by for_relation()
        self.customize_statements = []
        self.customize_results = []

        # Init operations
        self.select_op = self.SelectOperation(query, Model)
        self.filter_op = self.FilterOperation(query, Model)
        self.sort_op = self.SortOperation(query, Model)
        self.skiplimit_op = self.SkipLimitOperation(query, Model)

        # Init loader
        # May be replaced by for_relation()
        self.loader = self.PrimaryQueryLoader()

        # Resolve every input
        resolve_query_object(self.query, self.Model)

        # Init related executors: QueryExecutor() for every selected relation
        self.related_executors = {
            # Relation name => QueryExecutor
            relation.name: self.__class__(
                query=relation.query,
                Model=relation.property.mapper.class_,
            )._for_relation(
                # Init as a related executor
                source_executor=self,
                relation=relation,
            )
            for relation in self.query.select.relations.values()
        }

    def _for_relation(self, source_executor: QueryExecutor, relation: SelectedRelation):
        """ Init: Query Executor for a related object

        This function is called after __init__() for related executors: that is, executors that execute join operation
        and load related objects.

        Args:
            source_executor: The parent executor
            relation: The relation we're loading
        """
        # Load path: parent + (relation, Model)
        self.load_path = source_executor.load_path + (relation.name, unaliased_class(self.Model))

        # Replace the loader: use a Related Loader that can populate objects with related fields
        source_Model = source_executor.Model
        self.loader = self.RelatedQueryLoader(relation, source_Model, self.Model)

        # SkipLimit needs to enter a special pagination mode:
        # ordinary SkipLimit would ruin result sets, so it would use a window function to restrict results per main object.
        self.skiplimit_op.paginate_over_foreign_keys(relation.property.remote_side)

        # Copy customization handlers.
        # The function should be capable of dealing with `load_path` and thus handle every special case
        self.customize_statements = source_executor.customize_statements
        self.customize_results = source_executor.customize_results

        # Done
        return self

    def fetchall(self, connection: sa.engine.Connection) -> list[SARowDict]:
        """ Execute all queries and fetch result rows, including relations.

        This query would load primary objects on the current level, then collect these objects as "states", and
        recursively execute related Query Executors to fetch relations.
        They, in turn, would load related objects on their levels.
        """
        # Load all results: on the current level
        states = list(self._load_results(connection))

        # Load all relations:
        # gather objects from the current level as "states",
        self._load_relations(connection, states)

        # Customize results: execute every handler
        for handler in self.customize_results:
            states = handler(self, states)

        # Done
        return states

    # Overridable classes: loaders
    # Replace to customize the way data is loaded
    PrimaryQueryLoader = PrimaryQueryLoader
    RelatedQueryLoader = RelatedQueryLoader

    # Overridable classes: operations
    # Replace to customize how operations are executed
    SelectOperation = operations.SelectOperation
    FilterOperation = operations.FilterOperation
    SortOperation = operations.SortOperation
    SkipLimitOperation = operations.SkipLimitOperation

    def _load_results(self, connection: sa.engine.Connection) -> abc.Iterator[SARowDict]:
        """ Build a SELECT statement and fetch results

        For the current level only; no relations are loaded
        """
        # Build the SELECT statement
        stmt = self.statement()

        # Loader: execute the statement, fetch rows
        yield from self.loader.load_results(stmt, connection)

    def _load_relations(self, connection: sa.engine.Connection, states: list[SARowDict]):
        """ Load relations for the given states of this level

        Args:
            connection: The connection to use
            states: Loaded objects, to be populated with related fields
        """
        # Go through every selected relation
        for relation_name, executor in self.related_executors.items():
            # Give the loader the "states" to be populated with this nested loading
            executor.loader.for_states(states)

            # Fetch related objects
            # Results are discarded here because `executor.loader` has inserted them into `states` by now
            results = executor.fetchall(connection)

    def statement(self) -> sa.sql.Select:
        """ Build an SQL SELECT statement for the current Model.

        Build a statement that loads objects on the current level only: not for relations.

        NOTE that it may be very inconvenient to inspect/modify the query: some advanced operations, like skip/limit,
        may use subqueries and thus make it impossible to apply further filtering.

        Use `customize_statements()` to catch the statement while it's still fresh.
        """
        # Prepare a boilerplate statement for the current model
        # It has no selected fields yet.
        stmt = sa.select([]).select_from(self.Model)

        # Apply operations to this statement
        # This is where more clauses are applied.
        stmt = self._apply_operations_to_statement(stmt)

        # Done
        return stmt

    def all_statements(self) -> abc.Iterator[sa.sql.Select]:
        """ Build SQL statements for this model and all related objects

        This method is mainly used for debugging to see what SQL is generated.
        """
        # This model
        yield self.statement()

        # Every related executor
        for executor in self.related_executors.values():
            yield from executor.all_statements()

    def _apply_operations_to_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        """ Apply all operations to the statement """
        # The loader may want to modify the statement: add some columns, for instance
        stmt = self.loader.prepare_statement(stmt)

        # Operations: apply
        stmt = self.select_op.apply_to_statement(stmt)
        stmt = self.filter_op.apply_to_statement(stmt)
        stmt = self.sort_op.apply_to_statement(stmt)

        # Customization handlers: apply
        # This is done before `skiplimit` spoils the query
        for handler in self.customize_statements:
            stmt = handler(self, stmt)

        # Apply `skiplimit` last: it may use a subquery.
        # If this happens, no handler would know how to reference aliased columns properly
        stmt = self.skiplimit_op.apply_to_statement(stmt)

        # Done
        return stmt


# The type for load paths
# Example:
#   (User,)
#   (User, 'articles', Article)
#   (User, 'articles', Article, 'comments', Comment)
LoadPath = tuple[Union[type, str], ...]

# A callable that customizes a statement
CustomizeStatementCallable = abc.Callable[[QueryExecutor, sa.sql.Select], sa.sql.Select]

# A callable that
CustomizeResultsCallable = abc.Callable[[QueryExecutor, list[SARowDict]], list[SARowDict]]
