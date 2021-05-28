""" Jessiql Query Object: the object you can query with """

from __future__ import annotations

from dataclasses import dataclass

from typing import Optional, Union, TypedDict


@dataclass
class QueryObject:
    select: SelectQuery
    filter: FilterQuery
    sort: SortQuery
    skip: SkipQuery
    limit: LimitQuery

    __slots__ = 'select', 'filter', 'sort', 'skip', 'limit'

    @classmethod
    def from_query_object(cls, query_object: QueryObjectDict):
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


class QueryObjectDict(TypedDict):
    select: Optional[list[Union[str, dict]]]
    join: Optional[dict]
    filter: Optional[dict]
    sort: Optional[list[str]]
    skip: Optional[int]
    limit: Optional[int]


# Import structures for individual fields
from .select import SelectQuery
from .filter import FilterQuery
from .sort import SortQuery
from .skip import SkipQuery
from .limit import LimitQuery
