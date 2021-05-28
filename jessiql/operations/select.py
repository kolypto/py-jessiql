from collections import abc

import sqlalchemy as sa

from jessiql.query_object import QueryObject, SelectQuery
from jessiql.sainfo.columns import resolve_column_by_name
from jessiql.sautil.adapt import LeftRelationshipColumnsAdapter
from jessiql.typing import SAModelOrAlias


class SelectOperation:
    def __init__(self, query: QueryObject, target_Model: SAModelOrAlias):
        self.query = query
        self.target_Model = target_Model

    def apply_to_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        # Select columns from query.select
        # These are the columns that the user has requested
        stmt = stmt.add_columns(
            *select_fields(self.query.select, self.target_Model, where='select')
        )

        # Add columns that relationships want using query.select
        # Note: duplicate columns will be removed automatically by the select() method
        stmt = stmt.add_columns(
            *select_local_columns_for_relations(self.query, self.target_Model, where='select')
        )

        # Done
        return stmt


def select_fields(select: SelectQuery, Model: SAModelOrAlias, *, where: str) -> abc.Iterator[sa.sql.ColumnElement]:
    for field in select.fields.values():
        yield resolve_column_by_name(field.name, Model, where=where)


def select_local_columns_for_relations(q: QueryObject, Model: SAModelOrAlias, *, where: str):
    for relation in q.select.relations.values():
        adapter = LeftRelationshipColumnsAdapter(Model, relation.property)
        yield from adapter.replace_many(relation.property.local_columns)
