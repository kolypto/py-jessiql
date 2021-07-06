""" SqlAlchemy adapters: replace references with aliases """

import sqlalchemy as sa
from collections import abc

from jessiql.typing import SAModelOrAlias


class SimpleColumnsAdapter:
    """ Adapter for column names

    If you have an expression specified with the model class itself (e.g. primary key columns), but in your expression
    an aliased class should be used, this adapter will convert all references to point to that aliased class.

    Example:
        adapter = SimpleColumnsAdapter(Model)
        pk_cols = adapter.replace_many(query_info.pk_cols)
    """
    def __init__(self, Model: SAModelOrAlias):
        """ Init adapter to Model as alias

        Args:
            Model: Aliased class
        """
        adapter = sa.orm.util.ORMAdapter(Model)
        self._replace = adapter.replace

    def replace(self, obj):
        """ Adapt a single expression to use the aliased class """
        return sa.sql.visitors.replacement_traverse(obj, {}, self._replace)

    def replace_many(self, objs: abc.Iterable) -> abc.Iterator:
        """ Adapt a list of expressions to use the aliased class """
        yield from (
            self.replace(obj)
            for obj in objs
        )


class LeftRelationshipColumnsAdapter(SimpleColumnsAdapter):
    """ Adapter for related models.

    Only used when foreign keys for loaded relationships are inserted
    """
    def __init__(self, left_model: SAModelOrAlias, relation_property: sa.orm.RelationshipProperty):
        right_mapper = relation_property.mapper
        adapter = sa.orm.util.ORMAdapter(left_model, equivalents=right_mapper._equivalent_columns if right_mapper else {})
        self._replace = adapter.replace
