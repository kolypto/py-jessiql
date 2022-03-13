""" Query Object: the "select" subtree """

from __future__ import annotations

from collections import abc
from dataclasses import dataclass

from functools import cached_property
from typing import Union, Optional

import sqlalchemy as sa
import sqlalchemy.orm

from jessiql import exc
from jessiql.typing import SAAttribute, saproperty
from jessiql.sainfo.names import field_name
from jessiql.util.dataclasses import dataclass_notset
from jessiql.util.funcy import collecting

from .base import OperationInputBase


@dataclass
class SelectQuery(OperationInputBase):
    """ Query Object operation: the "select" operation

    Supports:
    * Columns
    * @property
    * @hybrid_property
    """
    # Selected fields: map
    fields: dict[str, SelectedField]
    # Selected relations: map
    relations: dict[str, SelectedRelation]

    def __init__(self, fields: abc.Iterable[SelectedField], relations: abc.Iterable[SelectedRelation]):
        # Fields, map
        self.fields = {field.name: field for field in fields}

        # Relations, map
        self.relations = {relation.name: relation for relation in relations}

    @cached_property
    def fields_and_relations(self) -> dict[str, Union[SelectedField, SelectedRelation]]:
        """ Get a mapping of both fields and relations """
        return {
            **self.fields,
            **self.relations,
        }

    @cached_property
    def names(self) -> frozenset[str]:
        """ Get the set of selected field and relation names """
        return frozenset(self.fields) | frozenset(self.relations)

    def __contains__(self, field: Union[str, SAAttribute]):
        """ Check if a field (column or relationship) is selected

        Args:
             field: Name or instrumented attribute
        """
        return field_name(field) in self.names

    @classmethod
    def from_query_object(cls, select: list[Union[str, dict]], join: dict[str, dict]):  # type: ignore[override]
        # Check types
        if not isinstance(select, list):
            raise exc.QueryObjectError(f'"select" must be an array')
        if not isinstance(join, dict):
            raise exc.QueryObjectError(f'"join" must be an array')

        # Tell fields and relations apart
        field: Union[str, dict]
        fields: list[SelectedField] = []
        relations: list[SelectedRelation] = []

        for field in (*select, join):
            # str: 'field_name'
            if isinstance(field, str):
                fields.append(SelectedField(name=field))  # type: ignore[call-arg]
            # dict: {'field_name': QueryObject}
            elif isinstance(field, dict):
                relations.extend(
                    SelectedRelation(name=name, query=QueryObject.from_query_object(query))  # type: ignore[call-arg,type-arg]
                    for name, query in field.items()
                )
            # anything else: not supported
            else:
                raise exc.QueryObjectError(f'Unsupported type encountered in "select": {field!r}')

        # Construct
        return cls(fields=fields, relations=relations)

    @collecting
    def export(self) -> abc.Iterator[Union[str, dict]]:
        yield from self.fields.keys()
        if self.relations:
            yield {
                relation.name: relation.query.dict()
                for relation in self.relations.values()
            }

    def export_select(self):
        return list(self.fields.keys())

    def export_join(self):
        return {
            relation.name: relation.query.dict()
            for relation in self.relations.values()
        }


@dataclass_notset('property', 'is_array', 'is_json', 'is_property', 'property_loads')
@dataclass
class SelectedField:
    name: str

    # Populated when resolved by resolve_selected_field()
    property: Union[sa.orm.ColumnProperty, saproperty]
    is_array: bool
    is_json: bool
    is_property: bool
    property_loads: Optional[abc.Sequence[str]]

    __slots__ = 'name', 'property', 'is_array', 'is_json', 'is_property', 'property_loads'

    def export(self) -> str:
        return self.name


@dataclass_notset('property', 'uselist')
@dataclass
class SelectedRelation:
    name: str
    query: QueryObject  # nested query

    # Populated when resolved by resolve_selected_relation()
    property: sa.orm.RelationshipProperty
    uselist: bool

    __slots__ = 'name', 'query', 'property', 'uselist'

    def export(self) -> dict:
        return {self.name: self.query.dict()}


from .query_object import QueryObject
