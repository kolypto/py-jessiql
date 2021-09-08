from typing import Optional
from collections import abc

import sqlalchemy as sa

from jessiql.typing import SAAttribute


def parse_dot_notation(input: str) -> tuple[str, Optional[tuple[str, ...]]]:
    """ Parse dot-notation

    Example:
        parse_dot_notation('a') #-> 'a', None
        parse_dot_notation('a.b.c') #-> 'a', ['b', 'c']
    """
    name, _, sub_path = input.partition('.')
    sub_path = tuple(sub_path.split('.')) if sub_path else None
    return name, sub_path


def json_field_subpath(expr: SAAttribute, sub_path: abc.Iterable[str]) -> sa.sql.elements.BinaryExpression:
    """ Return an expression that represents JSON object sub-path accessor

    Example:
        json_field_subpath(User.meta, 'a.b.c')
        # -> meta #> ('a', 'b', 'c')
    """
    # Postgres supports `Model.field['a', 'b', 'c']`!
    return expr[sub_path]  # TODO: (tag:postgres-only) this expression is only supported by PostgreSQL


def json_field_subpath_as_text(expr: SAAttribute, sub_path: abc.Iterable[str]) -> sa.sql.elements.BinaryExpression:
    """ Return an expression that represents JSON object sub-path accessor, as TEXT

    Example:
        json_field_subpath(User.meta, 'a.b.c')
        # -> meta #>> ('a', 'b', 'c')
    """
    # Postgres supports `Model.field['a', 'b', 'c']`!
    return expr[sub_path].astext  # TODO: (tag:postgres-only) this expression is only supported by PostgreSQL
