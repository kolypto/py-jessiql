from __future__ import annotations

import graphql
import dataclasses
from collections import abc
from typing import Optional


@dataclasses.dataclass
class FieldQueryInfo:
    """ Information about how to include a field

    The `field_query` function, given a GraphQL name, will decide how to map the field to an SqlAlchemy model.
    A field may resolve to multiple columns and go into the "select" portion,
    or it may resolve to a relationship and go as "join_name".

    Note that a GraphQL field may resolve to multiple columns (e.g. properties), or a single relationship.
    """
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
#   FieldQueryInfo
FieldQueryFunc = abc.Callable[[str, graphql.GraphQLField, tuple[str, ...]], Optional[FieldQueryInfo]]  # type: ignore[misc]


# A function that renames a field
FieldQueryRenameFunc = abc.Callable[[str, graphql.GraphQLField, tuple[str, ...]], str]


# An empty FieldQueryInfo object that causes the field to be skipped
QUERY_FIELD_SKIP = FieldQueryInfo()
