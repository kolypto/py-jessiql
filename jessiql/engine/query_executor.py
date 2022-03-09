""" QueryExecutor: an object that binds operations together to execute a Query Object

This is low level. See Query.
"""

from __future__ import annotations

from collections import abc
from typing import Union, Optional

import sqlalchemy as sa

from jessiql import operations
from jessiql.query_object import QueryObject, SelectedRelation
from jessiql.query_object.resolve import resolve_query_object
from jessiql.sainfo.models import unaliased_class
from jessiql.typing import SAModelOrAlias, SARowDict

from .loader import QueryLoaderBase, PrimaryQueryLoader, RelatedQueryLoader
from .settings import QuerySettings


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

    # The Query settings
    settings: QuerySettings

    # Load path: (Model, 'attribute', ...) path to the current model
    # Examples:
    # > (User,)
    # > (User, 'articles', Article)
    # > (User, 'articles', Article, 'comments', Comment)
    # Use it in your customization handler to know where you are.
    load_path: LoadPath

    # Query customization handlers: functions to alter `sa.sql.Select` statement.
    # This is your last chance to make changes to the statement.
    #
    # Mutable list: you can append your own custom handlers.
    # NOTE: your function will be called for the parent statement and related statements as well!
    #   It should be ready to inspect the `QueryExecutor.path` to find out where it is!
    #
    # Callback args:
    #   (self, statement)
    # Example usage: provide additional filtering, e.g. for security
    # Example usage:
    #   @query.customize_statements.append
    #   def security_filter(query: QueryExecutor, stmt: sa.sql.Select) -> sa.sql.Select:
    #       if query.path == (User,):
    #           return stmt.filter(...)
    customize_statements: list[CustomizeStatementCallable]

    # Results customization handler.
    # This is your last chance to catch results from every individual query.
    #
    # Mutable list: you can append your own custom handlers.
    # NOTE: your function will be called for the parent statement and related statements as well!
    #   It should be ready to inspect the `QueryExecutor.path` to find out where it is!
    #
    # Callback args:
    #   (self, list[row dict])
    # Example usage: pre-process loaded related objects
    # Example usage:
    #   @query.customize_results.append
    #   def preprocess_results(query: QueryExecutor, rows: list[dict]) -> list[dict]:
    customize_results: list[CustomizeResultsCallable]

    # Loader used to fetch results from the statement.
    # Typically,
    # * a PrimaryQueryLoader (for the root query), or
    # * a RelatedQueryLoader (that can populate objects with related fields)
    # Is replaced when _for_relation() is used
    loader: QueryLoaderBase

    # Child executors for related objects
    # For instance, when "join" is used, holds a QueryExecutor for every relationship by name
    related_executors: dict[str, QueryExecutor]

    def __init__(self, query: QueryObject, Model: SAModelOrAlias, settings: QuerySettings = None):
        """ Initialize a Query Executor for the given Query Object

        Args:
            query: The Query Object to execute
            Model: The SqlAlchemy Model class to execute the query against
        """
        # The query and the model to query against
        assert isinstance(query, QueryObject)
        self.query = query
        self.Model = Model
        self.settings = settings or self.DEFAULT_SETTINGS

        # Customization handlers
        # May be modified by for_relation()
        self.customize_statements = [self.settings.customize_statement]
        self.customize_results = [self.settings.customize_result]

        # Load path
        # May be modified by for_relation()
        self.load_path = (unaliased_class(Model),)

        # Init loader
        # May be replaced by for_relation()
        self.loader = self.PrimaryQueryLoader()

        # Init operations
        self.select_op = self.SelectOperation(query, Model, self.settings)
        self.filter_op = self.FilterOperation(query, Model, self.settings)
        self.sort_op = self.SortOperation(query, Model, self.settings)
        self.skiplimit_op = self.SkipLimitOperation(query, Model, self.settings)

        # Resolve every input
        resolve_query_object(self.query, self.Model)

        # Run for_query() on every operation
        # It is important that this is done after `self.query` is resolved!
        self.select_op.for_query(self)
        self.filter_op.for_query(self)
        self.sort_op.for_query(self)
        self.skiplimit_op.for_query(self)

        # Init related executors: QueryExecutor() for every selected relation
        self.related_executors = {
            # Relation name => QueryExecutor
            relation.name: self.__class__(
                query=relation.query,
                Model=relation.property.mapper.class_,
                settings=self.settings.get_relation_settings(relation.name)
            )._for_relation(
                # Init as a related executor
                source_executor=self,
                relation=relation,
            )
            for relation in self.query.select.relations.values()
        }

    __slots__ = (
        'query', 'Model', 'settings', 'load_path',
        'customize_statements', 'customize_results',
        'select_op', 'filter_op', 'sort_op', 'skiplimit_op',
        'loader', 'related_executors',
    )

    def _for_relation(self, source_executor: QueryExecutor, relation: SelectedRelation):
        """ Init: Query Executor for a related object

        This function is called after __init__() for related executors.
        It replaces the load path, the loader, and stuff.

        Perhaps, this is sub-optimal to initialize parameters twice. But the code is more readable this way.

        Args:
            source_executor: The parent executor
            relation: The relation we're loading
        """
        # Load path: parent + (relation, Model)
        self.load_path = source_executor.load_path + (relation.name, unaliased_class(self.Model))

        # Replace the loader: use a Related Loader that can populate objects with related fields
        self.loader = self.RelatedQueryLoader(relation, source_executor.Model, self.Model)

        # SkipLimit needs to enter a special pagination mode: window function pagination mode.
        # If it used SKIP/LIMIT, it would ruin result sets because "LIMIT 50" applies to the whole result set!
        # Whereas a window function limit would be able to restrict results per main object.
        self.skiplimit_op.paginate_over_foreign_keys(relation.property.remote_side)

        # Copy customization handlers.
        # These functions should be prepared in such a way that it inspects the `load_path` argument
        # and thus handles any related model no matter how deep down the tree it is.
        #
        # NOTE: we COPY the whole list object. By reference. Any modifications will propagate.
        self.customize_statements = source_executor.customize_statements
        self.customize_results = source_executor.customize_results

        # Done
        return self

    def fetchall(self, connection: sa.engine.Connection) -> list[SARowDict]:
        """ Execute all queries and fetch result rows, including relations.

        This query would:
        1. Load primary objects on the current level
        2. Collect these objects as "states" (e.g. objects with foreign key values)
        3. Recursively execute related Query Executors to fetch relations
        4. They, in turn would load related objects on their levels
        """
        # Load all results: on the current level
        states = list(self._load_results(connection))

        # Load all relations, using objects of the current level as "states"
        self._load_relations(connection, states)

        # Apply operations & customizations
        states = self._apply_operations_to_results(states)

        # Done
        return states

    def fetchone(self, connection: sa.engine.Connection) -> Optional[SARowDict]:
        """ Execute all queries and fetch exactly one result row, if available """
        # Get one row
        try:
            row = next(self._load_results(connection))
        except StopIteration:
            return None

        # Load relations, apply operations & customizations
        self._load_relations(connection, [row])
        states = self._apply_operations_to_results([row])

        # Done
        return row

    def count(self, connection: sa.engine.Connection) -> int:
        """ Execute the query and return the number of result rows only """
        # Prepare the statement
        stmt = sa.select([sa.func.count()]).select_from(self.Model)

        # Apply everything that may change the number of matching rows
        stmt = self.filter_op.apply_to_statement(stmt)
        for handler in self.customize_statements:
            stmt = handler(self, stmt)

        # Run
        res: sa.engine.CursorResult = connection.execute(stmt)
        return res.scalar()  # type: ignore[return-value]

    # Default settings object
    DEFAULT_SETTINGS = QuerySettings()

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
        """ Build a SELECT statement and fetch results for the current level

        Only this level. No relationships are loaded.
        """
        # Build the SELECT statement
        stmt = self.statement()

        # Loader: execute the statement, fetch rows
        yield from self.loader.load_results(stmt, connection)

    def _load_relations(self, connection: sa.engine.Connection, states: list[SARowDict]):
        """ Load relations for the given states of the current level

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

    @property
    def query_level(self) -> int:
        """ Get the "level" of this query

        Level 0: the root query
        Level 1: query that loads related objects
        Level 2: query that loads related objects of the next level
        """
        # First level: (Model,)
        # Second level: (Model, relation name, relationship property)
        return (len(self.load_path) - 1) // 2

    @property
    def limit(self) -> Optional[int]:
        """ Get the final LIMIT set on the query

        It may be changed because of:
        1. User query
        2. Default limit
        3. Max limit
        4. Some extension
        """
        return self.skiplimit_op.limit

    def statement(self) -> sa.sql.Select:
        """ Build an SQL SELECT statement for the current Model.

        Build a statement that loads objects on the current level only: not for relations.

        NOTE that it may be very inconvenient to inspect/modify the query: some advanced operations, like skip/limit,
        may wrap it into a subquery and thus make it impossible to apply further filtering.

        Use `self.customize_statements` to catch the statement while it's still fresh.
        """
        # Prepare a boilerplate statement for the current model
        # It has no selected fields yet.
        stmt = sa.select([]).select_from(self.Model)

        # Apply operations to this statement: select, filter, sort, skiplimit, etc, and customization too.
        # This is where more clauses are applied.
        stmt = self._apply_operations_to_statement(stmt)

        # Done
        return stmt

    def all_statements(self) -> abc.Iterator[sa.sql.Select]:
        """ Build SQL statements for this model and all related objects

        This method is mainly used for debugging to see the generated SQL.
        """
        # This model
        yield self.statement()

        # Every related executor
        for executor in self.related_executors.values():
            yield from executor.all_statements()

    def _apply_operations_to_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        """ Apply operations & handlers to the statement """
        # The loader may want to modify the statement: add some columns, for instance
        stmt = self.loader.prepare_statement(stmt)

        # Operations: apply
        stmt = self.select_op.apply_to_statement(stmt)
        stmt = self.filter_op.apply_to_statement(stmt)
        stmt = self.sort_op.apply_to_statement(stmt)

        # Customization handlers: apply
        # This is done before `skiplimit` spoils the query with its subqueries.
        for handler in self.customize_statements:
            stmt = handler(self, stmt)

        # Apply `skiplimit` last: it may use a subquery.
        # If this happens, no handler would know how to reference aliased columns properly
        stmt = self.skiplimit_op.apply_to_statement(stmt)

        # Done
        return stmt

    def _apply_operations_to_results(self, rows: list[SARowDict]) -> list[SARowDict]:
        """ Apply operations & handlers to the result set """
        # Apply operations
        for op in [self.select_op, self.filter_op, self.sort_op, self.skiplimit_op]:
            rows = op.apply_to_results(self, rows)

        # Apply customization handlers
        for handler in self.customize_results:
            rows = handler(self, rows)

        # Done
        return rows


# The type for load paths
# Example:
#   (User,)
#   (User, 'articles', Article)
#   (User, 'articles', Article, 'comments', Comment)
LoadPath = tuple[Union[type, str], ...]  # type: ignore[misc]

# A callable that customizes a statement
CustomizeStatementCallable = abc.Callable[[QueryExecutor, sa.sql.Select], sa.sql.Select]

# A callable that
CustomizeResultsCallable = abc.Callable[[QueryExecutor, list[SARowDict]], list[SARowDict]]


def reference_and_extend(source: list, extend: list) -> list:
    source.extend(extend)
    return source
