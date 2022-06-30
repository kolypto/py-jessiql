""" Implementation for field types & SQL handlers: how to query, filter, select them """

from .base import FieldHandlerBase, NameContext
from .base import Selectable, Sortable, Filterable

from .column import ColumnHandler
from .property import PropertyHandler
from .hybrid_property import HybridPropertyHandler
from .relation import RelationHandler


# TODO: this is probably not the best place for this code, but anyway, here it is. For now.
#   Move to QuerySettings and support custom fields?

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

    # Relationship handler: only works for filter & sort.
    # Does not conflict with other handlers, order does not matter.
    # We put it last simply because it's not used too often.
    RelationHandler,
)


from typing import Optional, TypeVar
from jessiql import exc, sainfo
from jessiql.typing import SAModelOrAlias


def choose_selectable_handler_or_fail(name: str, sub_path: Optional[tuple[str, ...]], Model: SAModelOrAlias) -> Selectable:
    """ Choose a handler for a field that will be selected """
    return _choose_handler_or_fail(name, sub_path, Model, context=NameContext.SELECT, HandlerType=Selectable)


def choose_sortable_handler_or_fail(name: str, sub_path: Optional[tuple[str, ...]], Model: SAModelOrAlias) -> Sortable:
    """ Choose a handler for a field that will be sorted by """
    return _choose_handler_or_fail(name, sub_path, Model, context=NameContext.SORT, HandlerType=Sortable)


def choose_filterable_handler_or_fail(name: str, sub_path: Optional[tuple[str, ...]], Model: SAModelOrAlias) -> Filterable:
    """ Choose a handler for a field that will be filtered by """
    return _choose_handler_or_fail(name, sub_path, Model, context=NameContext.FILTER, HandlerType=Filterable)


T = TypeVar('T')


def _choose_handler_or_fail(name: str, sub_path: Optional[tuple[str, ...]], Model: SAModelOrAlias,
                            context: NameContext, HandlerType: type[T]) -> T:
    """ Given a field, find a handler that implements it. Otherwise, fail. """
    for handler in ALL_HANDLERS:
        if issubclass(handler, HandlerType) and handler.is_applicable(name, sub_path, Model, context=context):
            return handler(name, sub_path, Model, context=context)  # type: ignore[return-value]
    else:
        raise exc.InvalidColumnError(sainfo.names.model_name(Model), name, where=context.value)
