""" Tools to resolve query object references into actual SqlAlchemy attributes

These methods take QueryObject classes and "resolve" them: augment them with information from SqlAlchemy models
"""

from __future__ import annotations

import sqlalchemy as sa
import sqlalchemy.orm
import sqlalchemy.sql.elements
from sqlalchemy.orm import (  # type: ignore[attr-defined]  # sqlalchemy stubs not updated
    InstrumentedAttribute,
)

from jessiql.sainfo.columns import resolve_column_by_name, is_array, is_json
from jessiql.sainfo.relations import resolve_relation_by_name
from jessiql.typing import SAModelOrAlias

from . import (
    SelectedField,
    SelectedRelation,
    SortingField,
    SortingDirection,
    FilterExpressionBase,
    FieldExpression,
    BooleanExpression,
)


def resolve_selected_field(Model: SAModelOrAlias, field: SelectedField, *, where: str) -> InstrumentedAttribute:
    attribute = resolve_column_by_name(Model, field.name, where=where)

    # Populate the missing fields
    field.property = attribute.property
    field.is_array = is_array(attribute)
    field.is_json = is_json(attribute)

    return attribute


def resolve_sorting_field(Model: SAModelOrAlias, field: SortingField, *, where: str) -> InstrumentedAttribute:
    attribute = resolve_column_by_name(Model, field.name, where=where)

    # Populate the missing fields
    field.property = attribute.property

    return attribute


def resolve_sorting_field_with_direction(Model: SAModelOrAlias, field: SortingField, *, where: str) -> sa.sql.ColumnElement:
    attribute = resolve_sorting_field(Model, field, where=where)

    if field.direction == SortingDirection.DESC:
        return attribute.desc()
    else:
        return attribute.asc()


def resolve_selected_relation(Model: SAModelOrAlias, field: SelectedRelation, *, where: str) -> InstrumentedAttribute:
    attribute = resolve_relation_by_name(Model, field.name, where=where)

    # Populate the missing fields
    field.property = attribute.property
    field.uselist = field.property.uselist

    return attribute


def resolve_filtering_expression(Model: SAModelOrAlias, expression: FilterExpressionBase, *, where: str) -> None:
    if isinstance(expression, FieldExpression):
        resolve_filtering_field_expression(Model, expression, where=where)
    elif isinstance(expression, BooleanExpression):
        for clause in expression.clauses:
            resolve_filtering_expression(Model, clause, where=where)
    else:
        raise NotImplementedError(repr(expression))


def resolve_filtering_field_expression(Model: SAModelOrAlias, expression: FieldExpression, *, where: str) -> InstrumentedAttribute:
    attribute = resolve_column_by_name(Model, expression.field, where=where)

    # Populate the missing fields
    expression.property = attribute.property
    expression.is_array = is_array(attribute)
    expression.is_json = is_json(attribute)

    return attribute
