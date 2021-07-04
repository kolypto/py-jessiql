from collections import abc

import sqlalchemy as sa

from .base import Operation
from jessiql.sainfo.columns import resolve_column_by_name
from jessiql.query_object import SortQuery, SortingDirection
from jessiql.typing import SAModelOrAlias


# TODO: expose a control that lets the user choose between NULLS FIRST and NULLS LAST?
#   It may be a 'column+' for default NULLS LAST, and 'column++' for NULLS FIRST?
#   With Postgres:
#   > By default, null values sort as if larger than any non-null value;
#   > that is, NULLS FIRST is the default for DESC order, and NULLS LAST otherwise.


class SortOperation(Operation):
    def apply_to_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        # Sort fields
        stmt = stmt.order_by(*self.compile_columns())

        # Done
        return stmt

    def compile_columns(self) -> abc.Iterator[sa.sql.ColumnElement]:
        yield from get_sort_fields_with_direction(self.query.sort, self.target_Model, where='sort')


def get_sort_fields_with_direction(sort: SortQuery, Model: SAModelOrAlias, *, where: str) -> abc.Iterator[sa.sql.ColumnElement]:
    for field in sort.fields:
        attribute = resolve_column_by_name(field.name, Model, where=where)

        if field.direction == SortingDirection.DESC:
            yield attribute.desc().nullslast()
        else:
            yield attribute.asc().nullslast()
