from __future__ import annotations

from collections import abc
from typing import TYPE_CHECKING

import sqlalchemy as sa

from jessiql.query_object import SelectQuery
from jessiql.sautil.adapt import LeftRelationshipColumnsAdapter
from jessiql.util.sacompat import add_columns_if_missing
from jessiql.typing import SAModelOrAlias

from .base import Operation


if TYPE_CHECKING:
    from jessiql.engine.query_executor import QueryExecutor


class SelectOperation(Operation):
    """ Select operation: add columns to the statement

    Handles: QueryObject.select
    When applied to a statement:
    * Adds SELECT column names that the user selected
    * Adds SELECT column names that are required for loading related objects
    """

    def apply_to_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        """ Modify the Select statement: add SELECT fields """
        # Add columns to Select
        # This includes our columns and foreign keys for related objects as well!
        selected_columns = list(self.compile_columns())
        stmt = add_columns_if_missing(stmt, selected_columns)

        # If no columns were selected, use the primary key
        # This is because SQL does not tolerate empty queries.
        # We could have used constant `1`, but where's fun in that :)
        if not selected_columns:
            primary_key = sa.orm.class_mapper(self.target_Model).primary_key
            stmt = add_columns_if_missing(stmt, primary_key)

        # Done
        return stmt

    def compile_columns(self) -> abc.Iterator[sa.sql.ColumnElement]:
        """ Generate the list of columns to be loaded by this query

        This includes our columns and foreign keys for related objects as well!
        """
        # Select columns from query.select
        # These are the columns that the user has requested
        for field in self.query.select.fields.values():
            yield from field.handler.select_columns(self.target_Model)

        # Add columns that relationships want using query.select
        # Note: duplicate columns will be removed automatically by the select() method
        yield from select_local_columns_for_relations(self.query.select, self.target_Model, where='select')

    def apply_to_results(self, query_executor: QueryExecutor, rows: list[dict]) -> list[dict]:
        for field in self.query.select.fields.values():
            rows = field.handler.apply_to_results(rows)
        return rows


def select_local_columns_for_relations(select: SelectQuery, Model: SAModelOrAlias, *, where: str):
    """ Get the list of columns required to load related objects: i.e. primary & foreign keys

    Args:
        select: QueryObject.select
        Model: the model to resolve the fields against
        where: location identifier for error reporting
    """
    # Go over every relationship
    for relation in select.relations.values():
        # Prepare to adapt the statement: i.e. rewrite it using aliased table names
        adapter = LeftRelationshipColumnsAdapter(Model, relation.property)

        # Resolve a relationship to a list of columns that should be loaded
        yield from adapter.replace_many(relation.property.local_columns)
