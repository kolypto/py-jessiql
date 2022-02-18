""" Query Field: functions that decide how a requested field is included into the Query Object """

from typing import Optional
import graphql

from ..query_object_argument import has_query_argument
from .defs import FieldQueryInfo


def query_every_field(field_name: str, field_def: graphql.type.definition.GraphQLField, path: tuple[str, ...]) -> Optional[FieldQueryInfo]:
    """ Field query func: include all fields as either 'select' or 'join'

    This function assumes every field to be a column and includes them as 'select'.
    But if a GraphQL field has a Query Object argument, it's included as 'join'.

    This is a very naïve function: it does not know whether your SqlAlchemy model has the field or not.
    """
    # How to handle? Field? Relation? Nothing?
    include_as_join = has_query_argument(field_def)
    include_as_select = not include_as_join

    # Done
    if include_as_select:
        return FieldQueryInfo(select=[field_name])
    elif include_as_join:
        return FieldQueryInfo(join_name=field_name)
    else:
        raise NotImplementedError
