""" JessiQL Query Object: the object you can query with """

from __future__ import annotations

from dataclasses import dataclass

from typing import Optional, Union, TypedDict
from jessiql.typing import SAModelOrAlias


class QueryObjectDict(TypedDict):
    """ Dict representation of a query object """
    select: Optional[list[Union[str, dict]]]
    join: Optional[dict]
    filter: Optional[dict]
    sort: Optional[list[str]]
    skip: Optional[int]
    limit: Optional[int]


@dataclass
class QueryObject:
    """ Query Object: a parsed Query Object

    When the Query Object is constructed from the input, fields are parsed, but not yet resolved:
    that is, it is not known whether they actually exist.

    First step, a Query Object is parsed into parts, each implemented as a Query Input:
    select, filter, sort, skip, limit.

    To resolve fields against a specific model or aliased class, resolve_query_object() must be used.
    It resolves every reference to a model's attribute and gathers information about real fields.
    """
    select: SelectQuery
    filter: FilterQuery
    sort: SortQuery
    skip: SkipQuery
    limit: LimitQuery

    __slots__ = 'select', 'filter', 'sort', 'skip', 'limit'

    @classmethod
    def from_query_object(cls, query_object: QueryObjectDict):
        """ Construct a Query Object from a query object dict

        Args:
            query_object: A query object dict you might've gotten from the client's request
        """
        return QueryObject(
            select=SelectQuery.from_query_object(
                select=query_object.get('select') or [],
                join=query_object.get('join') or {},
            ),
            filter=FilterQuery.from_query_object(
                filter=query_object.get('filter') or {},
            ),
            sort=SortQuery.from_query_object(
                sort=query_object.get('sort') or [],
            ),
            skip=SkipQuery.from_query_object(
                skip=query_object.get('skip'),
            ),
            limit=LimitQuery.from_query_object(
                limit=query_object.get('limit'),
            ),
        )

    def resolve(self, Model: Union[type, SAModelOrAlias]):
        """ Resolve this query object: resolve references to actual columns of the given model

        Note that unless this is done, the data within this Query Object is incomplete.
        """
        resolve.resolve_query_object(self, Model)
        return self


# Import structures for individual fields
from .select import SelectQuery
from .filter import FilterQuery
from .sort import SortQuery
from .skip import SkipQuery
from .limit import LimitQuery
from . import resolve
