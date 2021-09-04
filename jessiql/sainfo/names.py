from typing import Union

try:
    # Python 3.9+
    from functools import cache
except ImportError:
    # Python 3.8
    from functools import lru_cache as cache

import sqlalchemy as sa
import sqlalchemy.orm

from jessiql.typing import SAModelOrAlias, SAAttribute
from .models import unaliased_class


def model_name(Model: SAModelOrAlias) -> str:
    """ Get the name of the Model for this class """
    # We can't do `Model.__name__` because we can be given a type of an aliased class
    return unaliased_class(Model).__name__


@cache
def field_name(field: Union[str, SAAttribute]) -> str:
    """ Get the name of the field """
    # Get the name
    if isinstance(field, sa.orm.attributes.InstrumentedAttribute):
        return field.key
    elif isinstance(field, str):
        return field
    else:
        raise NotImplementedError(field)
