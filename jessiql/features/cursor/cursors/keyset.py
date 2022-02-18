from __future__ import annotations

import operator
from dataclasses import dataclass
from typing import Optional, NamedTuple

import sqlalchemy as sa

from jessiql import exc
from jessiql.sainfo.version import SA_14
from jessiql.typing import SARowDict, SAModelOrAlias
from jessiql.engine.query import Query
from jessiql.query_object import QueryObject, SortingDirection
from jessiql.sainfo.columns import (
    is_column_property_nullable,
    is_column_property_unique, resolve_column_by_name,
)

from .encode import encode_opaque_cursor, decode_opaque_cursor
from .base import CursorImplementation, PageLinks


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


class KeysetCursorData(NamedTuple):
    """ Cursor data for the "skip" cursor """
    # How many rows have we skipped?
    # Is only provided for compatibility with skip cursors
    skip: int

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
        return {'skip': self.skip, 'limit': self.limit, 'cols': self.cols, 'op': self.op, 'val': self.val}

    def encode(self) -> str:
        return encode_opaque_cursor('keys', self.serialize())

    @classmethod
    def decode(cls, cursor: str):
        type, data = decode_opaque_cursor(cursor)
        return cls(**data)


class KeysetCursor(CursorImplementation[KeysetPageInfo, KeysetCursorData]):
    """ Cursor implementation: "keyset". Uses smart keyset pagination to efficiently paginate a query """
    name = 'keys'
    PageInfo = KeysetPageInfo
    CursorData = KeysetCursorData

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
        if not (query.sort.names <= query.select.names):
            debug and print(f'Keyset n/a: some sort fields are not in select: {list(query.sort.names - query.select.names)}')
            return False

        # Yes we can!!
        return True

    def generate_cursor_links(self, skip: int, limit: int) -> PageLinks:
        page_info = self.page_info

        # Prepare the prev page link
        prev: Optional[str]
        if page_info.has_prev_page:
            prev = KeysetCursorData(
                skip=max(skip - limit, 0),
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
                skip=skip + limit,
                limit=limit,
                cols=page_info.column_names,
                op='>' if page_info.sort_asc else '<',
                val=page_info.last_tuple,  # type: ignore[arg-type]
            ).encode()
        else:
            next = None

        return PageLinks(prev=prev, next=next)

    @classmethod
    def apply_to_statement(cls, query: QueryObject, target_Model: SAModelOrAlias, stmt: sa.sql.Select, skip: int, limit: int, cursor: Optional[KeysetCursorData]) -> sa.sql.Select:
        # Prepare the filter expression
        if cursor:
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
            query_offset = None
        else:
            filter_expression = True
            query_offset = skip

        # Paginate
        # We will always load one more row to check if there's a next page
        if SA_14:
            return stmt.filter(filter_expression).offset(query_offset).limit(limit + 1)
        else:
            return stmt.where(filter_expression).offset(query_offset).limit(limit + 1)

    @classmethod
    def inspect_data_rows(cls, cursor_value: Optional[KeysetCursorData], query_executor: Query, rows: list[SARowDict]) -> KeysetPageInfo:
        # Decode cursor
        limit: Optional[int]
        if cursor_value is not None:
            limit = cursor_value.limit
        else:
            limit = query_executor.query.limit.limit

        # Columns that participate in keyset pagination
        column_names = tuple(field.name for field in query_executor.query.sort.fields)

        # Sort direction
        # We can be certain that all sort columns are sorted in the same direction, so we just take the first one
        sort_direction = query_executor.query.sort.fields[0].direction

        # If we have a cursor, it must have come from the previous
        has_prev_page = cursor_value is not None

        # No rows?
        if not rows or limit is None:
            return KeysetPageInfo(
                column_names=column_names,
                sort_asc=sort_direction == SortingDirection.ASC,
                first_tuple=None,
                last_tuple=None,
                has_prev_page=has_prev_page,
                has_next_page=False,
            )

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
        return KeysetPageInfo(
            column_names=column_names,
            sort_asc=sort_direction == SortingDirection.ASC,
            first_tuple=first_tuple,
            last_tuple=last_tuple,
            has_prev_page=has_prev_page,
            has_next_page=has_next_page,
        )
