
from __future__ import annotations

from typing import Optional, TypeVar, Generic, ClassVar, TYPE_CHECKING

import sqlalchemy as sa

from jessiql.typing import SARowDict, SAModelOrAlias
from jessiql.query_object import QueryObject


if TYPE_CHECKING:
    from jessiql.engine.query_executor import QueryExecutor
    from .page_links import PageLinks


class CursorImplementation:
    # Name for this cursor. Used to indicate its type
    name: ClassVar[str]

    # Cursor string value
    cursor: Optional[str]

    # Limit: how many items to include per page.
    # This is the input value: either from the "input" field, or from the decoded "cursor" value,
    limit: Optional[int]

    def __init__(self, cursor: Optional[str], *, limit: Optional[int]):
        self.cursor = cursor
        self.limit = limit

    __slots__ = 'cursor', 'limit'

    @classmethod
    def pagination_possible(cls, query: QueryObject) -> bool:
        """ Check: would this cursor support the current query?

        Some cursors may have prerequisites that make it impossible to use them.
        For instance, keyset pagination requires the results to be sorted by some unique key.
        """
        raise NotImplementedError

    def get_page_links(self) -> PageLinks:
        """ Generate cursor values for prev and next pages """
        raise NotImplementedError

    def apply_to_statement(self, query: QueryObject, target_Model: SAModelOrAlias, stmt: sa.sql.Select) -> sa.sql.Select:
        """ Modify the SQL Select statement that produces resulting rows """
        raise NotImplementedError

    def inspect_data_rows(self, query_executor: QueryExecutor, rows: list[SARowDict]):
        """ Inspect the result set """
        raise NotImplementedError
