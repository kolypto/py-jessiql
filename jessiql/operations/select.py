from __future__ import annotations

from collections import abc
from typing import TYPE_CHECKING

import sqlalchemy as sa
import sqlalchemy.orm

from jessiql.query_object import QueryObject, SelectQuery
from jessiql.sainfo.columns import resolve_column_by_name
from jessiql.sautil.adapt import LeftRelationshipColumnsAdapter
from jessiql.sautil.properties import evaluate_property_on_dict
from jessiql.util.sacompat import add_columns_if_missing
from jessiql.typing import SAModelOrAlias
from jessiql.sainfo.version import SA_13

from .base import Operation

if TYPE_CHECKING:
    from jessiql.engine.query_executor import QueryExecutor


class SelectOperation(Operation):
    """ Select operation: add columns to the statement

    Handles: QueryObject.select
    When applied to a statement:
    * Adds SELECT column names that the user selected
    * Adds SELECT column names that are required for loading related objects

    Supports:
    * Columns
    * @property
    * @hybrid_property
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
        yield from select_fields(self.query.select, self.target_Model, where='select')

        # Add columns that relationships want using query.select
        # Note: duplicate columns will be removed automatically by the select() method
        yield from select_local_columns_for_relations(self.query.select, self.target_Model, where='select')

    def apply_to_results(self, query_executor: QueryExecutor, rows: list[dict]) -> list[dict]:
        # Execute @property functions against the result
        properties = [
            field
            for field in self.query.select.fields.values()
            if field.is_property
        ]

        if properties:
            # For every row, evaluate @property-ies against it
            for row in rows:
                for field in properties:
                    # Assign new value
                    row[field.name] = evaluate_property_on_dict(field.property, row)  # type: ignore[arg-type]

        return rows


def select_fields(select: SelectQuery, Model: SAModelOrAlias, *, where: str) -> abc.Iterator[sa.sql.ColumnElement]:
    """ Get a list of columns that this QueryObject.select wants loaded

    It resolves every column by name and adds it to the query.

    Args:
        select: QueryObject.select
        Model: the model to resolve the fields against
        where: location identifier for error reporting
    """
    # Go over every field
    for field in select.fields.values():
        if field.is_property:
            # Resolve @property to columns using the information from @loads_attributes
            yield from (
                resolve_column_by_name(name, Model, where=where)
                for name in field.property_loads  # type: ignore[union-attr]
            )
        else:
            # Resolve it to a column
            yield resolve_column_by_name(field.name, Model, where=where)


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
