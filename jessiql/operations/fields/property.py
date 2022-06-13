from typing import Optional
from collections import abc
from dataclasses import dataclass

import sqlalchemy as sa

from jessiql import sainfo
from jessiql.sautil.properties import evaluate_property_on_dict
from jessiql.typing import SAModelOrAlias, saproperty
from .base import NameContext, FieldHandlerBase, Selectable


@dataclass
class PropertyHandler(Selectable):
    """ Hanlder for @property fields

    Notes:
    * It will only work for @property-ies that are properly annotated with @loads_attributes()
    * Properties will be evaluated magically against a row dict. This is a hack, don't expect much from it.
    * It supports hybrid properties, but will also evaluate them against a dict.
    """
    @classmethod
    def is_applicable(cls, name: str, sub_path: Optional[tuple[str, ...]], Model: SAModelOrAlias, context: NameContext) -> bool:
        # Context: @property values are only supported for SELECT context
        if context != context.SELECT:
            return False

        # Dot-notation is not supported
        if sub_path is not None:
            return False

        # Is it a @property?
        if not sainfo.properties.is_property(Model, name):
            return False

        # NOTE: the @property must also be properly annotated with @loads_attributes(), but we don't check it here.
        # Instead, we detect a property, and throw an error if it's used improperly.

        # Ok
        return True

    # Context: where the field is being used
    context: NameContext

    # Property name
    name: str

    # The property attribute
    property: saproperty

    # Information from @loads_attributes() decorator
    loads_attrs: abc.Sequence[str]

    def __init__(self, name: str, sub_path: Optional[tuple[str, ...]], Model: SAModelOrAlias, context: NameContext):
        # Resolve property.
        # Will rase an exception if it's not annotated properly
        Model = sainfo.models.unaliased_class(Model)
        prop = sainfo.properties.resolve_property_by_name(name, Model, where=context.value)  # will fail is the property is not annotated

        self.context = context
        self.name = name
        self.property = prop
        self.loads_attrs = sainfo.properties.get_property_loads_attribute_names(prop)  # type: ignore[assignment]

    __slots__ = 'context', 'name', 'property', 'loads_attrs'

    def select_columns(self, Model: SAModelOrAlias) -> abc.Iterator[sa.sql.ColumnElement]:
        for name in self.loads_attrs:
            yield sainfo.columns.resolve_column_by_name(name, Model, where=self.context.value)

    def apply_to_results(self, rows: list[dict]) -> list[dict]:
        for row in rows:
            row[self.name] = evaluate_property_on_dict(self.property, row)
        return rows

