""" Query Object: the "sort" operation """

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from functools import cached_property
from typing import Union

import sqlalchemy as sa

from jessiql import exc
from jessiql.sainfo.names import field_name
from jessiql.typing import SAAttribute
from jessiql.util.dataclasses import dataclass_notset

from .base import OperationInputBase


@dataclass
class SortQuery(OperationInputBase):
    """ Query Object operation: the "sort" operation """
    # The list of fields and directions to sort with
    # Note that the list is an ordered collection: order matters here
    fields: list[SortingField]

    @cached_property
    def names(self) -> frozenset[str]:
        """ Get a set of field names involved in sorting """
        return frozenset(field.name for field in self.fields)

    def __contains__(self, field: Union[str, SAAttribute]):
        """ Check if the field used in sorting

        Args:
             field: Name or instrumented attribute
        """
        return field_name(field) in self.names

    @classmethod
    def from_query_object(cls, sort: list[str]):  # type: ignore[override]
        # Check types
        if not isinstance(sort, list):
            raise exc.QueryObjectError(f'"sort" must be an array')

        # Construct
        fields = [cls._parse_input_field(field) for field in sort]
        return cls(fields=fields)

    def export(self) -> list[str]:
        return [
            field.export()
            for field in self.fields
        ]

    @staticmethod
    def _parse_input_field(field: Union[str]) -> SortingField:
        """ Parse a field string into a SortingField object """
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
        return SortingField(name=name, direction=direction)  # type: ignore[call-arg]


@dataclass_notset('property')
@dataclass
class SortingField:
    name: str
    direction: SortingDirection

    # Populated when resolved by resolve_sorting_field()
    property: sa.orm.ColumnProperty

    __slots__ = 'name', 'direction', 'property'

    def export(self) -> str:
        return f'{self.name}{self.direction.value}'


class SortingDirection(Enum):
    ASC = '+'
    DESC = '-'
