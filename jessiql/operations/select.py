import sqlalchemy as sa

from jessiql.query_object import QueryObject, resolve_selected_field, resolve_selected_relation
from jessiql.sautil.adapt import LeftRelationshipColumnsAdapter
from jessiql.typing import SAModelOrAlias


class SelectOperation:
    def __init__(self, query: QueryObject, target_Model: SAModelOrAlias):
        self.query = query
        self.target_Model = target_Model

    def apply_to_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        # Select columns from query.select
        stmt = stmt.add_columns(*(
            resolve_selected_field(self.target_Model, field, where='select')
            for field in self.query.select.fields.values()
        ))

        # Add columns that relationships want using query.select
        # Note: duplicate columns will be removed automatically by the select() method
        stmt = stmt.add_columns(
            *select_local_columns_for_relations(self.target_Model, self.query, where='select')
        )

        # Done
        return stmt


def select_local_columns_for_relations(Model: SAModelOrAlias, q: QueryObject, *, where: str):
    for relation in q.select.relations.values():
        relation_attribute = resolve_selected_relation(Model, relation, where=where)
        relation_property: sa.orm.RelationshipProperty = relation_attribute.property

        adapter = LeftRelationshipColumnsAdapter(Model, relation_property)
        yield from adapter.replace_many(relation_property.local_columns)
