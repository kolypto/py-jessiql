from __future__ import annotations

import operator
from dataclasses import dataclass
from typing import Optional, NamedTuple, TYPE_CHECKING

import sqlalchemy as sa

from jessiql import exc
from jessiql.sainfo.version import SA_14
from jessiql.typing import SARowDict, SAModelOrAlias
from jessiql.query_object import QueryObject, SortingDirection
from jessiql.sainfo.columns import (
    is_column_property_nullable,
    is_column_property_unique, resolve_column_by_name,
)

from .page_links import PageLinks
from .util import encode_opaque_cursor, decode_opaque_cursor
from .cursor_base import CursorImplementation


if TYPE_CHECKING:
    from jessiql.engine.query_executor import QueryExecutor


class KeysetCursor(CursorImplementation):
    """ Cursor implementation: "keyset". Uses smart keyset pagination to efficiently paginate a query """
    name = 'keys'

    # Decoded cursor value
    cursor_value: KeysetCursorData

    # Page info: becomes available after analyzing the result rows
    page_info: KeysetPageInfo

    def __init__(self, cursor: Optional[str], *, limit: Optional[int], **ignore_kwargs):
        super().__init__(cursor, limit=limit)

        # Parse the cursor
        if cursor is not None:
            self.cursor_value = KeysetCursorData.decode(cursor)

            # The 'limit' value has to either be empty or remain the same.
            # You can't change the limit midway.
            if self.limit not in (None, self.cursor_value.limit):
                raise exc.QueryObjectError(
                    'You cannot adjust the "limit" while using cursor-based pagination. '
                    'Either keep it constant, or replace with `null`.'
                )
            self.limit = self.cursor_value.limit
        else:
            self.cursor_value = None  # type: ignore[assignment]

        self.page_info = None  # type: ignore[assignment]

    __slots__ = 'cursor_value', 'page_info'

    @classmethod
    def pagination_possible(cls, query: QueryObject) -> bool:
        """ Check whether keyset pagination is possible

        Keyset pagination is possible when:
        * The query is sorted
        * Every field has exactly the same sorting
        * The final sort field is a non-nullable unique key
        * Sort fields are available with "select"
        """
        debug = 0  # change to inspect exactly why keyset pagination is not possible

        # The query is sorted
        if not query.sort.fields:
            debug and print(f'Keyset n/a: {query.sort.fields=} is empty')
            return False

        # All fields have the same sorting direction: all asc, or all desc
        # TODO: keyset pagination should also support sorting in varied directions!
        #  In this case, however, that nice tuple() expression would have to be replaced with something more complicated.
        #  We can do it!
        if len({field.direction for field in query.sort.fields}) != 1:
            debug and print(f'Keyset n/a: {query.sort.export()=} has varied directions')
            return False

        # The final sort field is a non-nullable unique key
        final_field = query.sort.fields[-1]
        if not is_column_property_unique(final_field.property) or is_column_property_nullable(final_field.property):
            debug and print(f'Keyset n/a: final sort field {final_field.name=} is not UNIQUE NOT NULL')
            return False

        # Every sort field is included in "select"
        # TODO: include manually?
        if not (query.sort.names <= query.select.names):
            debug and print(f'Keyset n/a: some sort fields are not in select: {list(query.sort.names - query.select.names)}')
            return False

        # Yes we can!!
        return True

    def get_page_links(self) -> PageLinks:
        page_info = self.page_info
        limit = self.cursor_value.limit if self.cursor_value else self.limit

        # Prepare the prev page link
        prev: Optional[str]
        if page_info.has_prev_page:
            prev = KeysetCursorData(
                limit=limit,
                cols=page_info.column_names,
                op='<' if page_info.sort_asc else '>',
                val=page_info.first_tuple,  # type: ignore[arg-type]
            ).encode()
        else:
            prev = None

        # Prepare the next page link
        next: Optional[str]
        if page_info.has_next_page:
            next = KeysetCursorData(
                limit=limit,
                cols=page_info.column_names,
                op='>' if page_info.sort_asc else '<',
                val=page_info.last_tuple,  # type: ignore[arg-type]
            ).encode()
        else:
            next = None

        return PageLinks(prev=prev, next=next)

    def apply_to_statement(self, query: QueryObject, target_Model: SAModelOrAlias, stmt: sa.sql.Select) -> sa.sql.Select:
        # Prepare the filter expression
        cursor = self.cursor_value

        if cursor is None:
            filter_expression = True
            limit = self.limit
        else:
            # Make sure the columns are still the same
            if set(cursor.cols) != query.sort.names:
                raise exc.QueryObjectError('You cannot adjust "sort" fields while using cursor-based pagination.')

            # Filter
            op = {'>': operator.gt, '<': operator.lt}[cursor.op]
            filter_expression = op(
                sa.tuple_(*(
                    resolve_column_by_name(field.name, target_Model, where='skip')
                    for field in query.sort.fields
                )),
                cursor.val
            )
            limit = cursor.limit

        if limit is None:
            return stmt

        # Paginate
        # We will always load one more row to check if there's a next page
        if SA_14:
            return stmt.filter(filter_expression).limit(limit + 1)
        else:
            return stmt.where(filter_expression).limit(limit + 1)

    def inspect_data_rows(self, query_executor: QueryExecutor, rows: list[SARowDict]):
        limit = self.cursor_value.limit if self.cursor_value else self.limit

        # Columns that participate in keyset pagination
        column_names = tuple(field.name for field in query_executor.query.sort.fields)

        # Sort direction
        # We can be certain that all sort columns are sorted in the same direction, so we just take the first one
        sort_direction = query_executor.query.sort.fields[0].direction

        # If we have a cursor, it must have come from the previous page
        has_prev_page = self.cursor_value is not None

        # No rows?
        if not rows or limit is None:
            self.page_info = KeysetPageInfo(
                column_names=column_names,
                sort_asc=sort_direction == SortingDirection.ASC,
                first_tuple=None,
                last_tuple=None,
                has_prev_page=has_prev_page,
                has_next_page=False,
            )
            return

        # Have next page?
        expected_count = limit + 1
        has_next_page = len(rows) == expected_count

        # Get tuples for the first/last rows
        first_tuple = tuple(rows[0][k] for k in column_names)
        last_tuple = tuple(rows[-2][k] for k in column_names) if has_next_page else None

        # We've loaded one extra row. Now remove it.
        if has_next_page:
            rows.pop()

        # Done
        self.page_info = KeysetPageInfo(
            column_names=column_names,
            sort_asc=sort_direction == SortingDirection.ASC,
            first_tuple=first_tuple,
            last_tuple=last_tuple,
            has_prev_page=has_prev_page,
            has_next_page=has_next_page,
        )


class KeysetCursorData(NamedTuple):
    """ Cursor data for the "skip" cursor """
    # The number of items per page
    limit: int

    # List of columns that are used for pagination.
    # Is only used to check that the user isn't tampering with request sorting
    cols: tuple[str, ...]

    # The operation to use for comparing the tuple: '>' or '<'
    op: str

    # The tuple used for keyset pagination
    val: tuple

    def serialize(self) -> dict:
        return {'limit': self.limit, 'cols': self.cols, 'op': self.op, 'val': self.val}

    def encode(self) -> str:
        return encode_opaque_cursor('keys', self.serialize())

    @classmethod
    def decode(cls, cursor: str):
        type, data = decode_opaque_cursor(cursor)
        return cls(**data)


@dataclass
class KeysetPageInfo:
    """ Page info for the "keyset" cursor """
    # Column names that participate in the pagination
    column_names: tuple[str, ...]

    # Sort direction: ASC? or DESC?
    sort_asc: bool

    # First row tuple of the window
    first_tuple: Optional[tuple]

    # Last row tuple of the window
    last_tuple: Optional[tuple]

    # Do we have any prev page?
    has_prev_page: bool

    # Do we have any next page?
    has_next_page: bool