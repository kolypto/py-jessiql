from typing import Optional
from collections import abc
from dataclasses import dataclass

import sqlalchemy as sa
import sqlalchemy.ext.hybrid

from jessiql import sainfo, exc
from jessiql.typing import SAModelOrAlias
from .base import NameContext, Selectable, Filterable, Sortable


@dataclass
class HybridPropertyHandler(Selectable, Filterable, Sortable):
    """ Handler for @hybrid_property fields.

    Unlike PropertyHandler, it will produce a fair SQL expression to handle the field properly inside the database.
    No evaluation against a dict.
    """

    @classmethod
    def is_applicable(cls, name: str, sub_path: Optional[tuple[str, ...]], Model: SAModelOrAlias, context: NameContext) -> bool:
        # Accept every hybrid property
        return sainfo.properties.is_hybrid_property(Model, name)

    # Context: where the field is being used
    context: NameContext

    # Property name
    name: str

    # The property attribute
    property: sa.ext.hybrid.hybrid_property

    # Property info: is it an array?
    # Used by: operation/filter
    is_array: bool

    # Property info: is it a JSON column?
    # Used by: operation/filter
    is_json: bool

    def __init__(self, name: str, sub_path: Optional[tuple[str, ...]], Model: SAModelOrAlias, context: NameContext):
        Model = sainfo.models.unaliased_class(Model)

        self.context = context
        self.name = name
        self.property = sainfo.properties.resolve_hybrid_property_by_name(name, Model, where=context.value)

        self.is_array = False  # TODO: override with a type annotation and allow array hybrid properties?
        self.is_json = False  # TODO: override with a type annotation and allow json hybrid properties with dot-notation?

        if sub_path is not None:
            raise exc.QueryObjectError(f'Field "{self.name}" does not support dot-notation yet: is a dynamic expression, not JSON')

    __slots__ = 'context', 'name', 'property', 'is_array', 'is_json'

    def select_columns(self, Model: SAModelOrAlias) -> abc.Iterator[sa.sql.ColumnElement]:
        yield self._refer_to(Model)

    def filter_by(self, Model: SAModelOrAlias) -> sa.sql.ColumnElement:
        return self._refer_to(Model)

    def sort_by(self, Model: SAModelOrAlias) -> sa.sql.ColumnElement:
        return self._refer_to(Model)

    def _refer_to(self, Model: SAModelOrAlias) -> sa.sql.ColumnElement:
        prop = sainfo.properties.resolve_hybrid_property_by_name(self.name, Model, where=self.context.value)
        # NOTE: in SqlAlchemy 1.3 a hybrid_property, when selected, gets name "anon_1". In 1.4, it gets a proper name automatically, but we still add a label() just to make sure
        return prop.label(self.name)  # type: ignore[attr-defined]
