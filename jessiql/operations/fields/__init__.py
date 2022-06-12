""" Implementation for field types & SQL handlers: how to query, filter, select them """

from .base import FieldHandlerBase, NameContext

from .column import ColumnHandler
from .property import PropertyHandler
from .hybrid_property import HybridPropertyHandler


# TODO: this is probably not the best place for this code, but anyway, here it is. For now.

# If you implement a custom handler, just add it here. All JessiQL instances will pick it up.
ALL_HANDLERS = (
    # Sequence matters.

    # ColumnHandler supports every column and expression, and does it well.
    # It does not matter where it is placed, but we put it first so that it matches faster (performance)
    ColumnHandler,

    # HybridPropertyHandler only supports @hybrid_property and not @property:
    # it is very specialized, so it goes first.
    HybridPropertyHandler,

    # PropertyHandler supports both plain @property and @hybrid_property, but has limited features.
    # So it has to go after HybridPropertyHandler, as a fallback.
    PropertyHandler,
)

from typing import Optional
from jessiql import exc, sainfo
from jessiql.typing import SAModelOrAlias


def choose_field_handler_or_fail(name: str, sub_path: Optional[tuple[str, ...]],
                           Model: SAModelOrAlias, context: NameContext) -> FieldHandlerBase:
    """ Given a field, find a handler that implements it. Otherwise, fail. """
    for handler in ALL_HANDLERS:
        is_applicable = handler.is_applicable(name, sub_path, Model, context=context)
        if is_applicable:
            return handler(name, sub_path, Model, context=context)
    else:
        raise exc.InvalidColumnError(sainfo.names.model_name(Model), name, where=context.value)
