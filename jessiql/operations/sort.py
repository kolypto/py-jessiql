from collections import abc

import sqlalchemy as sa

from .base import Operation
from jessiql.sainfo.columns import resolve_column_by_name
from jessiql.query_object import SortQuery, SortingDirection
from jessiql.typing import SAModelOrAlias


class SortOperation(Operation):
    def apply_to_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        # Sort fields
        stmt = stmt.order_by(
            *get_sort_fields_with_direction(self.query.sort, self.target_Model, where='sort')
        )

        # Done
        return stmt


def get_sort_fields_with_direction(sort: SortQuery, Model: SAModelOrAlias, *, where: str) -> abc.Iterator[sa.sql.ColumnElement]:
    for field in sort.fields:
        attribute = resolve_column_by_name(field.name, Model, where=where)

        if field.direction == SortingDirection.DESC:
            yield attribute.desc()
        else:
            yield attribute.asc()
