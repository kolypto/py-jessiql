from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import TypeDecorator
from sqlalchemy.sql.elements import Label

from sqlalchemy.orm import (
    CompositeProperty,
    ColumnProperty,
)
from sqlalchemy.orm import (  # type: ignore[attr-defined]  # sqlalchemy stubs not updated
    QueryableAttribute,
    InstrumentedAttribute,
    MapperProperty,
)

from jessiql.sainfo.names import model_name
from jessiql.typing import SAModelOrAlias, SAAttribute
from jessiql import exc


def resolve_column_by_name(Model: SAModelOrAlias, field_name: str, *, where: str) -> InstrumentedAttribute:
    # As simple as it looks, this code invokes __getattr__() on sa.orm.AliasedClass which adapts the SQL expression
    # to make sure it uses the proper aliased name in queries
    try:
        attribute = getattr(Model, field_name)
    except AttributeError as e:
        raise exc.InvalidColumnError(model_name(Model), field_name, where=where) from e

    # Check that it actually is a column
    if not is_column(attribute):
        raise exc.InvalidColumnError(model_name(Model), field_name, where=where)

    # Done
    return attribute


# region: Column types

def is_column(attribute: SAAttribute):
    return (
        is_column_property(attribute) or
        is_column_expression(attribute) or
        is_composite_property(attribute)
    )


def is_column_property(attribute: SAAttribute):
    return (
        isinstance(attribute, (InstrumentedAttribute, MapperProperty)) and
        isinstance(attribute.property, ColumnProperty) and
        isinstance(attribute.expression, sa.Column)  # not an expression, but a real column
    )


def is_column_expression(attribute: SAAttribute):
    return (
        isinstance(attribute, (InstrumentedAttribute, MapperProperty)) and
        isinstance(attribute.expression, Label)  # an expression, not a real column
    )


def is_composite_property(attribute: SAAttribute):
    return (
        isinstance(attribute, QueryableAttribute) and
        isinstance(attribute.property, CompositeProperty)
    )

# endregion


# region Column info

def get_column_type(attribute: SAAttribute) -> sa.types.TypeEngine:
    """ Get column's SQL type """
    if isinstance(attribute.type, TypeDecorator):
        # Type decorators wrap other types, so we have to handle them carefully
        return attribute.type.impl
    else:
        return attribute.type


def is_array(attribute: SAAttribute) -> bool:
    """ Is the attribute a PostgreSql ARRAY column? """
    return isinstance(get_column_type(attribute), sa.ARRAY)


def is_json(attribute: SAAttribute) -> bool:
    """ Is the attribute a PostgreSql JSON column? """
    return isinstance(get_column_type(attribute), sa.JSON)

# endregion
