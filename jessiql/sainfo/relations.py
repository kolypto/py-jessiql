from __future__ import annotations

import sqlalchemy as sa

from sqlalchemy.orm import (  # type: ignore[attr-defined]  # sqlalchemy stubs not updated
    InstrumentedAttribute,
    RelationshipProperty,
)
from sqlalchemy.orm import (  # type: ignore[attr-defined]  # sqlalchemy stubs not updated
    ONETOMANY,
    MANYTOONE,
    MANYTOMANY,
)
from sqlalchemy.orm.dynamic import DynaLoader

from jessiql.sainfo.format import model_name
from jessiql.typing import SAModelOrAlias, SAAttribute
from jessiql import query_object
from jessiql import exc


def resolve_selected_relation(Model: SAModelOrAlias, field: query_object.SelectedRelation, *, where: str) -> InstrumentedAttribute:
    attribute = resolve_relation_by_name(Model, field.name, where=where)

    # Populate the missing fields
    field.property = attribute.property
    field.uselist = field.property.uselist

    return attribute


def resolve_relation_by_name(Model: SAModelOrAlias, field_name: str, *, where: str) -> InstrumentedAttribute:
    try:
        attribute = getattr(Model, field_name)
    except AttributeError as e:
        raise exc.InvalidRelationError(model_name(Model), field_name, where=where) from e

    # Check that it actually is a column
    if not is_relation(attribute):
        raise exc.InvalidColumnError(model_name(Model), field_name, where=where)

    # Done
    return attribute


# region: Relation types

def is_relation(attr: SAAttribute):
    return (
        is_relation_relationship(attr) or
        is_relation_dynamic_loader(attr)
    )


def is_relation_relationship(attribute: SAAttribute):
    return (
        isinstance(attribute, InstrumentedAttribute) and
        isinstance(attribute.property, RelationshipProperty) and
        not isinstance(attribute.property.strategy, DynaLoader)
    )


def is_relation_dynamic_loader(attribute: SAAttribute):
    return (
        isinstance(attribute, InstrumentedAttribute) and
        isinstance(attribute.property, RelationshipProperty) and
        isinstance(attribute.property.strategy, DynaLoader)
    )

# endregion

# region Relation info



# endregion
