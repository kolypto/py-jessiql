""" Jessiql Query Object: the object you can query with """

from __future__ import annotations

from dataclasses import dataclass

from typing import Optional, Union, TypedDict


@dataclass
class QueryObject:
    select: Select
    sort: Sort

    @classmethod
    def from_query_object(cls, query_object: QueryObjectDict):
        return QueryObject(
            select=Select.from_query_object(
                select=query_object.get('select') or [],
                join=query_object.get('join') or {},
            ),
            sort=Sort.from_query_object(
                sort=query_object.get('sort') or [],
            ),
        )


class QueryObjectDict(TypedDict):
    select: Optional[list[Union[str, dict]]]
    join: Optional[dict]
    sort: Optional[list[str]]


# Import structures for individual fields
from .select import Select
from .sort import Sort
