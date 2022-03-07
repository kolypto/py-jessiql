""" Query Object: the "sort" operation """

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from functools import cached_property
from typing import Union, Optional

import sqlalchemy as sa
import sqlalchemy.orm

from jessiql import exc
from jessiql.sainfo.names import field_name
from jessiql.typing import SAAttribute
from jessiql.util.dataclasses import dataclass_notset
from jessiql.util.expressions import parse_dot_notation

from .base import OperationInputBase


@dataclass
class SortQuery(OperationInputBase):
    """ Query Object operation: the "sort" operation

    Supports:
    * Column names
    * JSON sub-objects (via dot-notation)
    """
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
    def _parse_input_field(field: str) -> SortingField:
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

        # Dot-notation?
        name, sub_path = parse_dot_notation(name)

        # Construct
        return SortingField(name=name, direction=direction, sub_path=sub_path)  # type: ignore[call-arg]


@dataclass_notset('property', 'is_json')
@dataclass
class SortingField:
    name: str
    direction: SortingDirection

    # Parsed dot-notation: "field.sub.sub"
    # Applicable to JSON fields
    sub_path: Optional[tuple[str, ...]]

    # Populated when resolved by resolve_sorting_field()
    property: sa.orm.ColumnProperty
    is_json: bool

    __slots__ = 'name', 'direction', 'sub_path', 'property', 'is_json'

    def export(self) -> str:
        return f'{self._field_expression()}{self.direction.value}'

    def _field_expression(self):
        if not self.sub_path:
            return self.name
        else:
            return '.'.join((self.name,) + self.sub_path)



class SortingDirection(Enum):
    ASC = '+'
    DESC = '-'
