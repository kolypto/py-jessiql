from __future__ import annotations

from typing import Optional, TypeVar, Generic, ClassVar, NamedTuple

import sqlalchemy as sa

from jessiql.typing import SARowDict, SAModelOrAlias
from jessiql.engine.query import Query
from jessiql.query_object import QueryObject


# A Page Info class: the data that you get after analyzing result rows
PageInfoT = TypeVar('PageInfoT')

# A Cursor Data class: the data that a cursor contains
CursorDataT = TypeVar('CursorDataT')


class PageLinks(NamedTuple):
    """ Links to the prev/next pages """
    # Link to the previous page, if available
    prev: Optional[str]

    # Link to the next page, if available
    next: Optional[str]


class CursorImplementation(Generic[PageInfoT, CursorDataT]):
    # Name for this cursor. Used to indicate its type
    name: ClassVar[str]

    # Which Page Info class to use
    PageInfo: ClassVar[type[PageInfoT]]

    # Which Cursor Data class to use
    CursorData: ClassVar[type[CursorDataT]]

    # Page info: the data got by analyzing result rows
    page_info: PageInfoT

    # Cursor Value: data that a cursor contains
    cursor_value: Optional[CursorDataT]

    def __init__(self, cursor_value: Optional[CursorDataT], page_info: PageInfoT):
        self.cursor_value = cursor_value
        self.page_info = page_info

    __slots__ = 'cursor_value', 'page_info'

    @classmethod
    def pagination_possible(cls, query: QueryObject) -> bool:
        """ Check: would this cursor support the current query?

        Some cursors may have prerequisites that make it impossible to use them.
        """
        raise NotImplementedError

    @classmethod
    def init_for_data_rows(cls, cursor_value: Optional[CursorDataT], query_executor: Query, rows: list[SARowDict]):
        """ Instantiate this object when result rows are available """
        return cls(cursor_value, cls.inspect_data_rows(cursor_value, query_executor, rows))

    def generate_cursor_links(self, skip: int, limit: int) -> PageLinks:
        """ Generate cursor values for prev and next pages """
        raise NotImplementedError

    @classmethod
    def apply_to_statement(cls, query: QueryObject, target_Model: SAModelOrAlias, stmt: sa.sql.Select, skip: int, limit: int, cursor: Optional[CursorDataT]) -> sa.sql.Select:
        """ Modify the SQL Select statement that produces resulting rows """
        raise NotImplementedError

    @classmethod
    def inspect_data_rows(cls, cursor_value: Optional[CursorDataT], query_executor: Query, rows: list[SARowDict]) -> PageInfoT:
        """ Inspect the result set and extract Page Info """
        raise NotImplementedError
