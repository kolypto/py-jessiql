from __future__ import annotations

import graphql
import dataclasses
from collections import abc
from typing import Optional


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


# An empty QueryFieldInfo object that causes the field to be skipped
QUERY_FIELD_SKIP = QueryFieldInfo()
