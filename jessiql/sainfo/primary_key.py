import sqlalchemy as sa
import sqlalchemy.orm


try:
    # Python 3.9+
    from functools import cache
except ImportError:
    # Python 3.8
    from functools import lru_cache as cache


@cache
def primary_key_names(Model: type) -> tuple[str, ...]:
    """ Get the list of primary key attribute names """
    return tuple(c.key for c in primary_key_columns(Model))  # type: ignore[misc]


@cache
def primary_key_columns(Model: type) -> tuple[sa.Column, ...]:
    """ Get the list of primary key attribute names """
    return tuple(c for c in sa.orm.class_mapper(Model).primary_key)
