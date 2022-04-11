""" Relay pagination """

from __future__ import annotations

import graphql
from collections import abc
from typing import Union, TypedDict, Optional

from jessiql import Query
from .query_object import query_object_for, QueryObject


def relay_query_object_for(info: graphql.GraphQLResolveInfo, nested_path: abc.Iterable[str] = ('edges', 'node'), *,
                           runtime_type: Union[str, graphql.GraphQLObjectType] = None,
                           first: int = None, after: str = None,
                           last: int = None, before: str = None,
                           ) -> QueryObject:
    """  """
    query_object = query_object_for(info, nested_path=nested_path, runtime_type=runtime_type)
    # TODO: Use `first`/`after` and insert them into `query_object`
    return query_object


def relay_query(query: Query) -> ConnectionDict:
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


class ConnectionSnakeDict(TypedDict):
    """ Relay Connection type, snake case """
    edges: list[EdgeDict]
    page_info: PageInfoSnakeDict


class EdgeDict(TypedDict):
    """ Relay Edge type: paginated item """
    node: Union[object, dict]
    cursor: Optional[str]


class PageInfoDict(TypedDict):
    """ Relay Page Info """
    hasPreviousPage: bool
    hasNextPage: bool
    startCursor: Optional[str]
    endCursor: Optional[str]


class PageInfoSnakeDict(TypedDict):
    """ Relay page into, snake case """
    has_previous_page: bool
    has_next_page: bool
    start_cursor: Optional[str]
    end_cursor: Optional[str]
