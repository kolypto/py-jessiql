from typing import Union

import sqlalchemy as sa
import sqlalchemy.orm

from jessiql.typing import SAModelOrAlias, SAAttribute


def model_name(Model: SAModelOrAlias) -> str:
    """ Get the name of the Model for this class """
    # We can't do `Model.__name__` because we can be given a type of an aliased class
    return Model.__mapper__.class_.__name__


def field_name(field: Union[str, SAAttribute]) -> str:
    """ Get the name of the field """
    # Get the name
    if isinstance(field, sa.orm.InstrumentedAttribute):  # type: ignore[attr-defined]  # sqlalchemy stubs not updated
        return field.key
    elif isinstance(field, str):
        return field
    else:
        raise NotImplementedError(field)
