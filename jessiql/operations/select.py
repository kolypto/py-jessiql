from collections import abc

import sqlalchemy as sa
import sqlalchemy.orm

from jessiql.query_object import QueryObject, SelectQuery
from jessiql.sainfo.columns import resolve_column_by_name
from jessiql.sautil.adapt import LeftRelationshipColumnsAdapter
from jessiql.typing import SAModelOrAlias
from .base import Operation


class SelectOperation(Operation):
    """ Select operation: add columns to the statement

    Handles: QueryObject.select
    When applied to a statement:
    * Adds SELECT column names that the user selected
    * Adds SELECT column names that are required for loading related objects
    """

    def __init__(self, query: QueryObject, target_Model: SAModelOrAlias):
        self.query = query
        self.target_Model = target_Model

    def apply_to_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        """ Modify the Select statement: add SELECT fields """
        # Add columns to Select
        # This includes our columns and foreign keys for related objects as well!
        selected_columns = list(self.compile_columns())
        stmt = stmt.add_columns(*selected_columns)

        # If no columns were selected, use the primary key
        # This is because SQL does not tolerate empty queries.
        # We could have used constant `1`, but where's fun in that :)
        if len(selected_columns) == 0:
            stmt = stmt.add_columns(
                *sa.orm.class_mapper(self.target_Model).primary_key
            )

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
