""" Relay pagination """

from __future__ import annotations

import os.path
import graphql
from collections import abc
from typing import Union, TypedDict, Optional

from jessiql.features.cursor import QueryPage, PageLinks
from .query_object import query_object_for, query_every_field
from .query_object import QueryFieldFunc, QueryObject
from .schema import pwd
from .schema import graphql_jessiql_schema  # noqa: shortcut

# Get this schema
with open(os.path.join(pwd, './relay.graphql'), 'rt') as f:
    graphql_relay_schema = f.read()


def relay_query_object_for(info: graphql.GraphQLResolveInfo, nested_path: abc.Iterable[str] = ('edges', 'node'), *,
                           runtime_type: Union[str, graphql.GraphQLObjectType] = None,
                           field_query: QueryFieldFunc = query_every_field,
                           first: int = None, after: str = None,
                           last: int = None, before: str = None,
                           ) -> QueryObject:
    """  """
    query_object = query_object_for(info, nested_path=nested_path, runtime_type=runtime_type, field_query=field_query)
    # TODO: Use `first`/`after` and insert them into `query_object`
    return query_object


def relay_query(query: QueryPage) -> ConnectionDict:
    """ Get results in Relay paginated format """
    return {
        'edges': [],  # TODO: insert `query` results here. Provide `cursor` for the first and the last item
        'pageInfo': {
            # TODO: insert relevant data
            'hasPreviousPage': True,
            'hasNextPage': True,
            'startCursor': None,
            'endCursor': None,
        }
    }


class ConnectionDict(TypedDict):
    """ Relay Connection type: paginated list """
    edges: list[EdgeDict]
    pageInfo: PageInfoDict


class EdgeDict(TypedDict):
    """ Relay Edge type: paginated item """
    node: object
    cursor: Optional[str]


class PageInfoDict(TypedDict):
    """ Relay Page Info """
    hasPreviousPage: bool
    hasNextPage: bool
    startCursor: Optional[str]
    endCursor: Optional[str]
