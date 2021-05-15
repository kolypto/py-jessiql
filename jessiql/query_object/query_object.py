""" Jessiql Query Object: the object you can query with """

from __future__ import annotations

from dataclasses import dataclass

from typing import Optional, Union, TypedDict


@dataclass
class QueryObject:
    select: Select

    @classmethod
    def from_query_object(cls, query_object: QueryObjectDict):
        return QueryObject(
            select=Select.from_query_object(
                select=query_object.get('select') or [],
                join=query_object.get('join') or {},
            )
        )


class QueryObjectDict(TypedDict):
    select: Optional[list[Union[str, dict]]]


from .select import Select
