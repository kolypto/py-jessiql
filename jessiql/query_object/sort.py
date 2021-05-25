""" Query Object: the "sort" operation """

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from functools import cached_property
from typing import Union

import sqlalchemy as sa
import sqlalchemy.orm

from jessiql import exc
from jessiql.sainfo.names import field_name
from jessiql.typing import SAAttribute
from jessiql.util.dataclasses import dataclass_notset


@dataclass
class Sort:
    fields: list[SortingField]

    @cached_property
    def names(self) -> frozenset[str]:
        return frozenset(field.name for field in self.fields)

    def __contains__(self, field: Union[str, SAAttribute]):
        return field_name(field) in self.names

    @classmethod
    def from_query_object(cls, sort: list[str]):
        # Check types
        if not isinstance(sort, list):
            raise exc.QueryObjectError(f'"sort" must be an array')

        # Construct
        fields = [cls.parse_input_field(field) for field in sort]
        return cls(fields=fields)

    @classmethod
    def parse_input_field(cls, field: Union[str]) -> SortingField:
        # Look at the ending character
        end_c = field[-1:]

        # If there's a sorting character, use it
        if end_c == '-' or end_c == '+':
            name = field[:-1]
            direction = SortingDirection(end_c)
        # Otherwise, use default sorting
        else:
            name = field
            direction = SortingDirection.ASC

        # Construct
        return SortingField(name=name, direction=direction)


@dataclass_notset('property')
@dataclass
class SortingField:
    name: str
    direction: SortingDirection

    # Populated when resolved by resolve_sorting_field()
    property: sa.orm.ColumnProperty

    __slots__ = 'name', 'direction', 'property'


class SortingDirection(Enum):
    ASC = '+'
    DESC = '-'
