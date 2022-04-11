
from __future__ import annotations

from typing import Optional, TYPE_CHECKING, Union

import sqlalchemy as sa

from jessiql import exc
from jessiql.operations.pager.page_links import PageLinks
from jessiql.typing import SAAttribute, SAModelOrAlias
from jessiql.query_object import QueryObject
from jessiql.sautil.adapt import SimpleColumnsAdapter
from jessiql.sainfo.version import SA_14

from jessiql.operations.base import Operation
from jessiql.operations.sort import get_sort_fields_with_direction
from jessiql.util.sacompat import add_columns

from .skiplimit import SkipLimitOperation
from .cursor_base import CursorImplementation
from .cursor_skip import SkipCursor
from .cursor_keyset import KeysetCursor


if TYPE_CHECKING:
    from jessiql.engine.settings import QuerySettings
    from jessiql.engine.query_executor import QueryExecutor


# Sorting direction
ASC, DESC = +1, -1


class BeforeAfterOperation(Operation):
    """ Before/After operation: pagination

    Handles: QueryObject.before, QueryObject.after, QueryObject.limit
    When applied to a statement:
    * Adds SKIP/LIMIT clauses
    """
    # String value of the cursor
    cursor: Optional[str]

    # Cursor handler object
    cursor_impl: CursorImplementation

    # Pagination direction: ASC, DESC
    direction: Optional[int]

    # How many items to include
    # Note that this is the "input limit": the limit the user have provided via the Query Object.
    # The final limit value is probably encoded inside the cursor
    limit: Optional[int]

    # Skip/Limit operation on the same object
    skiplimit_op: SkipLimitOperation

    def __init__(self, query: QueryObject, target_Model: SAModelOrAlias, settings: QuerySettings, skiplimit_op: SkipLimitOperation):
        super().__init__(query, target_Model, settings)

        # Prepare the limit
        self.skiplimit_op = skiplimit_op
        self.limit = self.settings.get_final_limit(self.skiplimit_op.limit)

        # Only one of these can be used, not simultaneously
        before = self.query.before.cursor
        after = self.query.after.cursor
        if sum([
            before is not None,
            after is not None,
            self.query.skip.skip is not None,
        ]) > 1:
            raise exc.QueryObjectError("Choose a pagination method and use either 'skip', or 'before', or 'after'.")

        # Set self.cursor, self.direction
        if before is not None:
            self.cursor = before
            self.direction = -1
        elif after is not None:
            self.cursor = after
            self.direction = +1
        else:
            self.cursor = None
            self.direction = None

    __slots__ = 'cursor', 'cursor_impl', 'direction', 'limit', 'skiplimit_op'

    def for_query(self, query_executor: QueryExecutor):
        # Initialize the cursor
        # It needs to be initialized here because `pagination_possible()` needs to see resolved columns

        # Cursor type
        if self.cursor is None:
            cls = KeysetCursor if KeysetCursor.pagination_possible(self.query) else SkipCursor
        else:
            cls = get_cursor_impl_cls(self.cursor)

        # Init cursor handler
        self.cursor_impl = cls(self.cursor, limit=self.limit, skip=self.skiplimit_op.skip)  # `skip` is only accepted by SkipCursor
        self.limit = self.cursor_impl.limit

    def get_page_links(self) -> PageLinks:
        """ Generate cursors to navigate to the next and previous pages """
        return self.cursor_impl.get_page_links()

    # Override parent methods

    def apply_to_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        return self.cursor_impl.apply_to_statement(self.query, self.target_Model, stmt)

    def apply_to_results(self, query_executor: QueryExecutor, rows: list[dict]) -> list[dict]:
        return self.cursor_impl.inspect_data_rows(query_executor, rows)



def get_cursor_impl_cls(cursor: str) -> type[CursorImplementation]:
    """ Given a cursor string, get a class that implements it, or fail """
    if cursor.startswith(SkipCursor.name):
        return SkipCursor
    elif cursor.startswith(KeysetCursor.name):
        return KeysetCursor
    else:
        raise NotImplementedError
