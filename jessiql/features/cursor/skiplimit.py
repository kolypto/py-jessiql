from __future__ import annotations

from typing import Optional, Union

import sqlalchemy as sa

from jessiql import exc, QueryObject
from jessiql.engine.query import Query
from jessiql.operations import SkipLimitOperation

from .cursors import get_cursor_impl, CursorImplementation
from .cursors import SkipCursor, KeysetCursor
from .cursors import CursorData
from .cursors import PageLinks


class CursorLimitOperation(SkipLimitOperation):
    """ An extension to SkipLimit operation that optionally supports cursors and keyset pagination.

    The first feature: opaque cursors.
    That is, the first time you paginate, you provide a query object with a limit:

        { limit: 10 }

    Then, optionally, you can get a cursor from this object:

        skip:eyJza2lwIjogMiwgImxpbWl0IjogMn0=

    And now you get to the next page using it:

        { skip: 'skip:eyJza2lwIjogMiwgImxpbWl0IjogMn0=' }

    When possible, it applies KEYSET pagination, which is the fastest way to paginate!
    If your "sort" ends with a unique key, and sort fields are also available in "select":

        { select: ['ctime', 'id'],
          sort: ['ctime-', 'id-'],
          limit: 10
        }

    Then the cursor will automatically use a keyset pagination mechanism which is way more stable and effective!
    """

    __slots__ = 'cursor_type', 'cursor_value', 'cursor_impl'

    # The value of the cursor: parsed named tuple, if provided
    cursor_value: Optional[CursorData]

    # The class that implements the cursor
    cursor_type: type[CursorImplementation]

    # Cursor implementation object.
    # Only becomes available after some data rows have been returned.
    cursor_impl: Optional[CursorImplementation]

    def for_query(self, query_executor: Query):
        # Parse the cursor
        try:
            self.cursor_type, self.cursor_value = cursor_value_input(self.query)
        except Exception as e:
            raise exc.QueryObjectError('The provided cursor is invalid') from e

        # Cursor implementation won't be available before data rows are processed
        self.cursor_impl = None

        # Register a function that will analyze result sets
        @query_executor.customize_results.append
        def inspect_final_row(query_executor: Query, rows: list[dict]):
            self.cursor_impl = self.cursor_type.init_for_data_rows(self.cursor_value, query_executor, rows)
            return rows

        # Done
        return self

    def get_page_links(self) -> PageLinks:
        """ Generate cursors to navigate to the next and previous pages """
        skip, limit = self._get_skip_limit()

        # No per-page limit set? No pagination possible. Quit.
        if not limit:
            return PageLinks(None, None)

        # Cursor implementation not initialized?
        # This means that data rows haven't been fetched yet.
        if not self.cursor_impl:
            raise RuntimeError(
                "It's not possible to generate cursors links before the result set rows are all fetched. "
                "Call fetchall() first."
            )

        # Pagination available. Go.
        return self.cursor_impl.generate_cursor_links(skip, limit)

    def _apply_cursor_pagination(self, stmt: sa.sql.Select):
        """ Modify the query: apply pagination using data from the cursor """
        skip, limit = self._get_skip_limit()

        # No limit is set at all. We cannot do anything special
        if not limit:
            return self._apply_simple_skiplimit_pagination(stmt)

        # The 'limit' value has to either be empty or remain the same.
        # You can't change the limit midway.
        if self.limit not in (None, limit):
            raise exc.QueryObjectError(
                'You cannot adjust the "limit" while using cursor-based pagination. '
                'Either keep it constant, or replace with `null`.'
            )

        # Paginate
        return self.cursor_type.apply_to_statement(self.query, self.target_Model, stmt, skip, limit, self.cursor_value)

    def _get_skip_limit(self) -> tuple[int, Optional[int]]:
        """ Get values for (skip, limit): either from the cursor or from the QueryObject """
        if self.cursor_value is None:
            return (
                self.skip or 0,
                self.limit if self.limit else None,
            )
        else:
            return self.cursor_value.skip, self.cursor_value.limit

    # Override parent methods

    def apply_to_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        if self._window_over_foreign_keys:
            raise exc.RuntimeQueryError('Sorry, cursor-based pagination is not yet supported for nested objects. Please use skip/limit')

        if self.cursor_type is None:
            return super().apply_to_statement(stmt)
        else:
            return self._apply_cursor_pagination(stmt)


def cursor_value_input(query: QueryObject) -> tuple[type[CursorImplementation], CursorData]:
    """ Parse the incoming query and get cursor type and value

    Returns:
        type: cursor type class
        value: cursor value, parsed named tuple, or None
    """
    cursor_raw_value: Optional[str] = query.skip.page

    if cursor_raw_value is None:
        cursor_type = KeysetCursor if KeysetCursor.pagination_possible(query) else SkipCursor
        cursor_value = None
    else:
        cursor_type = get_cursor_impl(cursor_raw_value)
        cursor_value = cursor_type.CursorData.decode(cursor_raw_value)

    return cursor_type, cursor_value
