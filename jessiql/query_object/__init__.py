""" Tools for parsing the Query Object

These classes only represent the internal structure of the Query Object.
They do not interact with SqlAlchemy in any way.
"""

from .query_object import QueryObject, QueryObjectDict

from .select import Select, SelectedField, SelectedRelation
from .sort import Sort, SortingField, SortingDirection

from .resolve import (
    resolve_selected_field,
    resolve_selected_relation,
    resolve_sorting_field,
    resolve_sorting_field_with_direction,
)
