""" Query Object: the "sort" operation """

from __future__ import annotations

from dataclasses import dataclass

import itertools
import sqlalchemy as sa
from typing import Any, Union

from jessiql import exc
from jessiql.util.dataclasses import dataclass_notset
from jessiql.util.funcy import collecting

from .base import OperationInputBase


@dataclass
class Filter(OperationInputBase):
    conditions: list[FilterExpressionBase]

    @classmethod
    def from_query_object(cls, filter: dict):
        # Check types
        if not isinstance(filter, dict):
            raise exc.QueryObjectError(f'"filter" must be an object')

        # Construct
        conditions = cls.parse_input_fields(filter)
        return cls(conditions=conditions)

    @classmethod
    @collecting
    def parse_input_fields(cls, condition: dict) -> list[FilterExpressionBase]:
        for key, value in condition.items():
            if key.startswith('$'):
                yield cls._parse_input_boolean_expression(key, value)
            else:
                yield from cls._parse_input_field_expressions(key, value)

    @classmethod
    def _parse_input_field_expressions(cls, field_name: str, value: Union[dict[str, Any], Any]):
        if not isinstance(value, dict):
            yield FieldFilterExpression(field=field_name, operator='$eq', value=value)
        else:
            for operator, operand in value.items():
                yield FieldFilterExpression(field=field_name, operator=operator, value=operand)

    @classmethod
    def _parse_input_boolean_expression(cls, operator: str, conditions: Union[dict, list[dict]]):
        # Check types
        # $not is the only unary operator
        if operator == '$not':
            if not isinstance(conditions, dict):
                raise exc.QueryObjectError(f"{operator}'s operand must be an object")

            conditions = [conditions]
        # Every other operator receives a list of conditions
        else:
            if not isinstance(conditions, list):
                raise exc.QueryObjectError(f"{operator}'s operand must be an array")

        # Construct
        return BooleanFilterExpression(
            operator=operator,
            clauses=list(itertools.chain.from_iterable(
                cls.parse_input_fields(condition)
                for condition in conditions
            ))
        )


class FilterExpressionBase:
    pass


@dataclass_notset('property', 'is_array', 'is_json')
@dataclass
class FieldFilterExpression(FilterExpressionBase):
    field: str
    operator: str
    value: Any

    # Populated when resolved by resolve_filtering_expression()
    property: sa.orm.ColumnProperty
    is_array: bool
    is_json: bool

    __slots__ = 'name', 'operator', 'value', 'property', 'is_array', 'is_json'


@dataclass
class BooleanFilterExpression(FilterExpressionBase):
    operator: str
    clauses: list[FilterExpressionBase]

    __slots__ = 'operator', 'clauses'
