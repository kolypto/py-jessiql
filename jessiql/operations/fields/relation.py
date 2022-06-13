
from typing import Optional
from collections import abc
from dataclasses import dataclass

import sqlalchemy as sa
import sqlalchemy.orm
from sqlalchemy.orm.attributes import InstrumentedAttribute

from jessiql import sainfo, exc
from jessiql.typing import SAModelOrAlias
from .base import NameContext, Filterable, Sortable


@dataclass
class RelationHandler(Filterable, Sortable):
    """ Handler for relationships.

    This handler does not select relationships: it only supports filtering by a related column.
    Example: {"user.articles.tags": ['sci-fi']}
    """

    @classmethod
    def is_applicable(cls, name: str, sub_path: Optional[tuple[str, ...]], Model: SAModelOrAlias, context: NameContext) -> bool:
        # Selecting is not supported: only filtering & sorting
        if context == context.SELECT:
            return False

        # Type
        attr = sainfo.relations.get_relation_by_name(name, Model)
        if not sainfo.relations.is_relation(attr):
            return False

        # `sub_path` must be provided: we cannot do anything with just a relationship name
        if not sub_path:
            return False

        # Ok
        return True

    # Context: where the field is being used
    context: NameContext

    # Relation name
    name: str

    # Dot-notation path, optional
    sub_path: tuple[str, ...]

    # Relationship attribute
    property: sa.orm.relationships.RelationshipProperty

    # Property info: is it an array?
    # NOTE: this refers not to the relationship itself -- but to the column that it refers to!!
    # Used by: operation/filter
    is_array: bool

    # Property info: is it a JSON column?
    # NOTE: this refers not to the relationship itself -- but to the column that it refers to!!
    # Used by: operation/filter
    is_json: bool

    def __init__(self, name: str, sub_path: Optional[tuple[str, ...]], Model: SAModelOrAlias, context: NameContext):
        Model = sainfo.models.unaliased_class(Model)
        attr = sainfo.relations.resolve_relation_by_name(name, Model, where=context.value)

        assert sub_path is not None
        self.context = context
        self.name = name
        self.sub_path = sub_path
        self.property = attr.property

        # Get information about the related attribute
        related_attribute = _follow_subpath_to_the_final_attribute(attr, sub_path, where=context.value)
        if not sainfo.columns.is_column(related_attribute):
            model_name = Model.__name__
            attr_name = '.'.join([self.name, *(self.sub_path or [])])
            raise exc.InvalidColumnError(model_name, attr_name, where=context.value)

        self.is_array = sainfo.columns.is_array(related_attribute)
        self.is_json = sainfo.columns.is_json(related_attribute)

    __slots__ = 'context', 'name', 'sub_path', 'property', 'is_array', 'is_json'

    def filter_by(self, Model: SAModelOrAlias) -> sa.sql.ColumnElement:
        relation = sainfo.relations.resolve_relation_by_name(self.name, Model, where=self.context.value)
        return _follow_subpath_to_the_final_attribute(relation, self.sub_path, where=self.context.value)

    def filter_with(self, Model: SAModelOrAlias, expr: sa.sql.ColumnElement) -> sa.sql.ColumnElement:
        return _build_related_condition(Model, (self.name, *self.sub_path), where=self.context.value, final_expr=expr)

    def sort_by(self, Model: SAModelOrAlias) -> sa.sql.ColumnElement:
        # TODO: implement sorting by related column: SELECT DISTINCT ON (pk) + JOIN. sort_by() must be able to modify the statement
        raise NotImplementedError


def _follow_subpath_to_the_final_attribute(attr: InstrumentedAttribute, sub_path: abc.Sequence[str], *, where: str) -> InstrumentedAttribute:
    """ Given a sub-path, go down to the referenced attribute.

    Example:
        _follow(User.articles, ['id']) -> Article.id
        _follow(User.articles, ['comments', 'id']) -> Comment.id

    Returns:
        The referenced attribute, be it a field, a relationship, or some sort of property.
        It's not aliased.
    """
    # Nowhere to descend? Stop recursion.
    if not sub_path:
        return attr

    # Get the next field name
    field_name, remaining_sub_path = sub_path[0], sub_path[1:]

    # Everything but the last element is a relationship
    if not isinstance(attr.property, sa.orm.RelationshipProperty):
        raise exc.InvalidRelationError(attr.parent.class_.__name__, field_name, where=where)

    # Get the related model and field
    try:
        related_model = attr.mapper.class_
        related_attr = getattr(related_model, field_name)
    except AttributeError:
        raise exc.InvalidColumnError(related_model.__name__, field_name, where=where)

    # Recurse
    return _follow_subpath_to_the_final_attribute(related_attr, remaining_sub_path, where=where)


def _build_related_condition(Model: SAModelOrAlias, sub_path: abc.Sequence[str], *, where: str, final_expr: sa.sql.ColumnElement) -> sa.sql.ColumnElement:
    """ Build a recursively nested condition for relationships

    Example:
        _build(User, ['articles.id'], Article.author_id == 1) ->
            User.articles.any(
                Article.author_id == 1
            )
    """
    # The last element is a field name.
    # All the preceding elements are relationship names.
    if len(sub_path) == 1:
        return final_expr

    # Descend into relations
    relation_name, remaining_sub_path = sub_path[0], sub_path[1:]
    relation = getattr(Model, relation_name)

    # Recurse
    related_model = relation.mapper.class_
    expr = _build_related_condition(related_model, remaining_sub_path, where=where, final_expr=final_expr)

    # Build condition
    if relation.property.uselist:
        return relation.any(expr)
    else:
        return relation.has(expr)
