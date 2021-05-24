""" SqlAlchemy adapters: replace references with aliases """

import sqlalchemy as sa
from collections import abc


class SimpleColumnsAdapter:
    def __init__(self, Model: type):
        adapter = sa.orm.util.ORMAdapter(Model)
        self._replace = adapter.replace

    def replace(self, obj):
        return sa.sql.visitors.replacement_traverse(obj, {}, self._replace)

    def replace_many(self, objs: abc.Iterable) -> abc.Iterator:
        yield from (
            self.replace(obj)
            for obj in objs
        )


class LeftRelationshipColumnsAdapter(SimpleColumnsAdapter):
    def __init__(self, left_model: type, relation_property: sa.orm.RelationshipProperty):
        right_mapper = relation_property.mapper
        adapter = sa.orm.util.ORMAdapter(left_model, equivalents=right_mapper._equivalent_columns if right_mapper else {})
        self._replace = adapter.replace
