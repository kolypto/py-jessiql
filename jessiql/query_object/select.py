""" Query Object: the "select" subtree """

from __future__ import annotations

from collections import abc
from dataclasses import dataclass

from functools import cached_property
from typing import Union

import sqlalchemy as sa
import sqlalchemy.orm

from jessiql import exc
from jessiql.typing import SAAttribute


@dataclass
class Select:
    fields: dict[str, SelectedField]
    relations: dict[str, SelectedRelation]

    def __init__(self, fields: abc.Iterable[SelectedField], relations: abc.Iterable[SelectedRelation]):
        self.fields = {field.name: field for field in fields}
        self.relations = {relation.name: relation for relation in relations}

    @cached_property
    def fields_and_relations(self) -> dict[str, Union[SelectedField, SelectedRelation]]:
        return {
            **self.fields,
            **self.relations,
        }

    def __contains__(self, field: Union[str, SAAttribute]):
        # Get the name
        if isinstance(field, sa.orm.InstrumentedAttribute):  # type: ignore[attr-defined]  # sqlalchemy stubs not updated
            field_name = field.key
        elif isinstance(field, str):
            field_name = field
        else:
            raise NotImplementedError(field)

        # Contains?
        return (
            field_name in self.fields or
            field_name in self.relations
        )

    @classmethod
    def from_query_object(cls, select: list[Union[str, dict]], join: dict[str, dict]):
        # Check types
        if not isinstance(select, list):
            raise exc.QueryObjectError(f'"select" must be an array')
        if not isinstance(join, dict):
            raise exc.QueryObjectError(f'"join" must be an array')

        # Combine
        input = [*select, join]

        # Tell fields and relations apart
        fields: list[SelectedField] = []
        relations: list[SelectedRelation] = []

        for field in input:
            # str: 'field_name'
            if isinstance(field, str):
                fields.append(SelectedField(name=field))
            # dict: {'field_name': QueryObject}
            elif isinstance(field, dict):
                relations.extend(
                    SelectedRelation(name=name, query=QueryObject.from_query_object(query))
                    for name, query in field.items()
                )
            # anything else: not supported
            else:
                raise exc.QueryObjectError(f'Unsupported type encountered in "select": {field!r}')

        # Construct
        return cls(fields=fields, relations=relations)


@dataclass
class SelectedField:
    name: str

    def __init__(self, name: str):
        self.name = name

    # Populated when resolved by resolve_selected_field()
    property: sa.orm.ColumnProperty
    is_array: bool
    is_json: bool

    __slots__ = ('name', 'property', 'is_array', 'is_json')


@dataclass
class SelectedRelation:
    name: str
    query: QueryObject

    def __init__(self, name: str, query: QueryObject):
        self.name = name
        self.query = query

    # Populated when resolved by resolve_selected_relation()
    property: sa.orm.ColumnProperty
    uselist: bool

    __slots__ = ('name', 'query', 'property', 'uselist')


from .query_object import QueryObject
