""" Tools to resolve query object references into actual SqlAlchemy attributes

These methods take QueryObject classes and "resolve" them: augment them with information from SqlAlchemy models
"""

from __future__ import annotations

import sqlalchemy as sa
import sqlalchemy.orm
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
