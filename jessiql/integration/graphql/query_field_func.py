""" Query Field: functions that decide how a requested field is included into the Query Object """
from __future__ import annotations

import graphql
import dataclasses
from collections import abc
from typing import Optional

from .query_object_argument import has_query_argument


@dataclasses.dataclass
class QueryFieldInfo:
    """ Information about how to include a field """
    # The list of field names to be added as "select" fields
    select: abc.Iterable[str] = dataclasses.field(default_factory=tuple)
    # The name of a field to be included as "join" field, if any
    join_name: Optional[str] = None


# A function to decide how a field should/not be included into the Query Object.
# Normally, it would inspect the SqlAlchemy model and some secure selection rules as well.
# Args:
#   field name
#   field definition
#   path: field names from the root
# Returns:
#   QueryFieldInfo
QueryFieldFunc = abc.Callable[[str, graphql.type.definition.GraphQLField, tuple[str]], QueryFieldInfo]


def query_field_default(field_name: str, field_def: graphql.type.definition.GraphQLField, path: tuple[str]) -> QueryFieldInfo:
    """ Query Field func: include all fields as either 'select' or 'join'

    Every field is included as Query Object 'select',
    unless it has a query argument. Then it's included as Query Object 'join'

    Note that this naive function does not even check whether fields exist on the SqlAlchemy model.
    """
    # How to handle? Field? Relation? Nothing?
    include_as_join = has_query_argument(field_def)
    include_as_select = not include_as_join

    # Done
    if include_as_select:
        return QueryFieldInfo(select=[field_name])
    elif include_as_join:
        return QueryFieldInfo(join_name=field_name)
    else:
        raise NotImplementedError


