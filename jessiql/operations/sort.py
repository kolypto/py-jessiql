from collections import abc
from typing import Union

import sqlalchemy as sa

from .base import Operation
from jessiql.query_object import SortQuery, SortingDirection
from jessiql.typing import SAModelOrAlias


# TODO: expose a control that lets the user choose between NULLS FIRST and NULLS LAST?
#   It may be a 'column+' for default NULLS LAST, and 'column++' for NULLS FIRST?
#   With Postgres:
#   > By default, null values sort as if larger than any non-null value;
#   > that is, NULLS FIRST is the default for DESC order, and NULLS LAST otherwise.


class SortOperation(Operation):
    """ Sort operation: define the ordering of result rows

    Handles: QueryObject.sort
    When applied to a statement:
    * Adds ORDER BY with columns and sorting defined by the user

    Supports:
    * Column names
    * JSON sub-objects (via dot-notation)
    """

    def apply_to_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        """ Modify the Select statement: add ORDER BY clause """
        # Sort fields
        stmt = stmt.order_by(*self.compile_columns())

        # Done
        return stmt

    def compile_columns(self) -> abc.Iterator[sa.sql.ColumnElement]:
        """ Generate the list of columns, sorted asc()/desc(), to be used in the query """
        yield from get_sort_fields_with_direction(self.query.sort, self.target_Model, where='sort')


def get_sort_fields_with_direction(sort: SortQuery, Model: SAModelOrAlias, *, where: str) -> abc.Iterator[sa.sql.ColumnElement]:
    """ Get the list of expressions to sort by

    Args:
        sort: QueryObject.sort
        Model: the model to resolve the fields against
        where: location identifier for error reporting
    """
    # Go over every field provided by the user
    for field in sort.fields:
        expr = field.handler.sort_by(Model)

        # Make a sorting expression, depending on the direction
        if field.direction == SortingDirection.DESC:
            yield expr.desc().nullslast()
        else:
            yield expr.asc().nullslast()
