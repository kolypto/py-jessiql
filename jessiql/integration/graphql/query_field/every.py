""" Query Field: functions that decide how a requested field is included into the Query Object """
import graphql

from ..query_object_argument import has_query_argument
from .defs import QueryFieldInfo


def query_every_field(field_name: str, field_def: graphql.type.definition.GraphQLField, path: tuple[str, ...]) -> QueryFieldInfo:
    """ Query Field func: include all fields as either 'select' or 'join'

    Every field is included as Query Object 'select',
    unless it has a query argument. Then it's included as Query Object 'join'

    This is a very naïve function. It includes every field into the query, even if it does not exist on the SqlAlchemy model.
    There is no way to exclude virtual fields: they will produce JessiQL errors.
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


