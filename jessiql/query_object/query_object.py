""" Jessiql Query Object: the object you can query with """

from __future__ import annotations

from dataclasses import dataclass

from typing import Optional, Union, TypedDict


@dataclass
class QueryObject:
    select: Select
    filter: Filter
    sort: Sort
    skip: Skip
    limit: Limit

    __slots__ = 'select', 'filter', 'sort', 'skip', 'limit'

    @classmethod
    def from_query_object(cls, query_object: QueryObjectDict):
        return QueryObject(
            select=Select.from_query_object(
                select=query_object.get('select') or [],
                join=query_object.get('join') or {},
            ),
            filter=Filter.from_query_object(
                filter=query_object.get('filter') or {},
            ),
            sort=Sort.from_query_object(
                sort=query_object.get('sort') or [],
            ),
            skip=Skip.from_query_object(
                skip=query_object.get('skip'),
            ),
            limit=Limit.from_query_object(
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
from .select import Select
from .filter import Filter
from .sort import Sort
from .skip import Skip
from .limit import Limit
