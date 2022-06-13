from typing import Optional
from collections import abc
from dataclasses import dataclass
import sqlalchemy as sa

from jessiql import exc, sainfo
from jessiql.typing import SAModelOrAlias
from jessiql.util.expressions import json_field_subpath_as_text
from .base import NameContext, Selectable, Filterable, Sortable


@dataclass
class ColumnHandler(Selectable, Filterable, Sortable):
    """ Handler for columns, column expressions, composite properties """
    @classmethod
    def is_applicable(cls, name: str, sub_path: Optional[tuple[str, ...]], Model: SAModelOrAlias, context: NameContext) -> bool:
        attr = sainfo.columns.get_column_by_name(name, Model)

        # Type ok?
        if attr is None or not sainfo.columns.is_column(attr):
            return False

        # Context: no dot-notation is supported for SELECT
        if context == context.SELECT and sub_path is not None:
            return False

        # Ok
        return True

    # Context: where the field is being used
    context: NameContext

    # Field name
    name: str

    # Dot-notation path, optional
    sub_path: Optional[tuple[str, ...]]

    # The SqlAlchemy property
    property: sa.orm.ColumnProperty

    # Property info: is it an array?
    # Used by: operation/filter
    is_array: bool

    # Property info: is it a JSON column?
    # Used by: operation/filter
    is_json: bool

    def __init__(self, name: str, sub_path: Optional[tuple[str, ...]], Model: SAModelOrAlias, context: NameContext):
        attribute = sainfo.columns.resolve_column_by_name(name, Model, where=context.value)

        self.context = context
        self.name = name
        self.sub_path = sub_path
        self.property = attribute.property
        self.is_array = sainfo.columns.is_array(attribute)
        self.is_json = sainfo.columns.is_json(attribute)

        if self.sub_path and not self.is_json:
            raise exc.QueryObjectError(f'Field "{self.name}" does not support dot-notation: not a JSON field')

    __slots__ = 'context', 'name', 'sub_path', 'property', 'is_array', 'is_json'

    def select_columns(self, Model: SAModelOrAlias) -> abc.Iterator[sa.sql.ColumnElement]:
        assert self.sub_path is None
        yield self._refer_to(Model)

    def filter_by(self, Model: SAModelOrAlias) -> sa.sql.ColumnElement:
        return self._refer_to(Model)

    def sort_by(self, Model: SAModelOrAlias) -> sa.sql.ColumnElement:
        return self._refer_to(Model)

    def _refer_to(self, Model: SAModelOrAlias) -> sa.sql.ColumnElement:
        expr = sainfo.columns.resolve_column_by_name(self.name, Model, where=self.context.value)

        if self.sub_path:
            assert self.is_json
            expr = json_field_subpath_as_text(expr, self.sub_path)  # type: ignore[assignment]

        return expr

