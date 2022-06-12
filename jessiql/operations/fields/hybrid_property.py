
from typing import Optional
from collections import abc
from dataclasses import dataclass

import sqlalchemy as sa
import sqlalchemy.ext.hybrid

from jessiql import sainfo, exc
from jessiql.typing import SAModelOrAlias
from .base import NameContext, FieldHandlerBase


@dataclass
class HybridPropertyHandler(FieldHandlerBase):
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

    def __init__(self, name: str, sub_path: Optional[tuple[str, ...]], Model: SAModelOrAlias, context: NameContext):
        Model = sainfo.models.unaliased_class(Model)

        self.context = context
        self.name = name
        self.property = sainfo.properties.resolve_hybrid_property_by_name(name, Model, where=context.value)

        if sub_path is not None:
            raise exc.QueryObjectError(f'Field "{self.name}" does not support dot-notation yet: is a dynamic expression, not JSON')

    __slots__ = 'context', 'name', 'property'

    def select_columns(self, Model: SAModelOrAlias) -> abc.Iterator[sa.sql.ColumnElement]:
        yield self.refer_to(Model)

    def refer_to(self, Model: SAModelOrAlias) -> abc.Iterable[sa.sql.ColumnElement]:
        # NOTE: in SqlAlchemy 1.3 a hybrid_property, when selected, gets name "anon_1". In 1.4, it gets a proper name automatically, but we still add a label() just to make sure
        return sainfo.properties.resolve_hybrid_property_by_name(self.name, Model, where=self.context.value).label(self.name)
