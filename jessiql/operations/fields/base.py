from collections import abc
from typing import Optional
from dataclasses import dataclass
from enum import Enum
import sqlalchemy as sa

from jessiql.typing import SAModelOrAlias


class NameContext(Enum):
    """ The context in which a field is used """
    SELECT = 'select'
    JOIN = 'join'
    FILTER = 'filter'
    SORT = 'sort'


@dataclass
class FieldHandlerBase:
    @classmethod
    def is_applicable(cls, name: str, sub_path: Optional[tuple[str, ...]], Model: SAModelOrAlias, context: NameContext) -> bool:
        """ Check whether this class can handle a particular field """
        raise NotImplementedError

    def __init__(self, name: str, sub_path: Optional[tuple[str, ...]], Model: SAModelOrAlias, context: NameContext):
        """ Initialize field handler from user input

        Args:
            input_name: Field input: string name, or dot-notation
            Model: The model to resolve the field against
            context: The context in which the field is going to be used
        """

    __slots__ = ()

    def select_columns(self, Model: SAModelOrAlias) -> abc.Iterator[sa.sql.ColumnElement]:
        """ Get columns to add to the select statement (select operation) """
        raise NotImplementedError

    def refer_to(self, Model: SAModelOrAlias) -> sa.sql.ColumnElement:
        """ Get an expression that's used to refer to this field in expressions (sort and filter) """
        raise NotImplementedError

    def apply_to_results(self, rows: list[dict]) -> list[dict]:
        """ Make adjustments to the result set in order to represent the field correctly """
        return rows


@dataclass
class Selectable:
    """ Mixin for fields that are selectable """


@dataclass
class Filterable:
    """ Mixin for fields that are filterable """
    is_array: bool
    is_json: bool


@dataclass
class Sortable:
    """ Mixin for fields that are sortable """
