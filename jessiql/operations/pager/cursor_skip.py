from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, NamedTuple, TYPE_CHECKING

import sqlalchemy as sa

from jessiql.typing import SARowDict, SAModelOrAlias
from jessiql.query_object import QueryObject
from jessiql import exc

from .page_links import PageLinks
from .cursor_base import CursorImplementation
from .util import encode_opaque_cursor, decode_opaque_cursor


if TYPE_CHECKING:
    from jessiql.engine.query_executor import QueryExecutor


class SkipCursor(CursorImplementation):
    """ Cursor implementation: "skip". Uses SKIP/LIMIT to paginate a query. """
    name = 'skip'

    # Decoded cursor value
    cursor_value: Optional[SkipCursorData]

    # Page info: becomes available after analyzing the result rows
    page_info: SkipPageInfo

    def __init__(self, cursor: Optional[str], *, skip: Optional[int], limit: Optional[int]):
        super().__init__(cursor, limit=limit)

        # Parse the cursor
        if cursor is not None:
            self.cursor_value = SkipCursorData.decode(cursor)

            # The 'limit' value has to either be empty or remain the same.
            # You can't change the limit midway.
            if self.limit not in (None, self.cursor_value.limit):  # type: ignore[union-attr]
                raise exc.QueryObjectError(
                    'You cannot adjust the "limit" while using cursor-based pagination. '
                    'Either keep it constant, or replace with `null`.'
                )
            self.limit = self.cursor_value.limit  # type: ignore[union-attr]
        elif skip is not None and limit is not None:
            self.cursor_value = SkipCursorData(skip=skip, limit=limit)
        elif limit is not None:
            self.cursor_value = None
            self.limit = limit
        else:
            self.cursor_value = None
            self.limit = None

        self.page_info = None  # type: ignore[assignment]

    __slots__ = 'cursor_value', 'page_info'

    @classmethod
    def pagination_possible(cls, query: QueryObject) -> bool:
        return True

    def get_page_links(self) -> PageLinks:
        # Page links can only be generated after the query is actually executed
        if self.page_info is None:
            raise RuntimeError(
                "It's not possible to generate cursors links before the result set rows are all fetched. "
                "Call fetchall() first."
            )

        # Limit
        skip = self.cursor_value.skip if self.cursor_value else 0
        limit = self.limit

        # If no limit is set, pagination is not possible
        if limit is None:
            return PageLinks(prev=None, next=None)

        # Prepare the prev page link
        prev: Optional[str]
        if skip <= 0:
            prev = None
        else:
            prev = SkipCursorData(
                skip=max(skip - limit, 0),
                limit=limit,
            ).encode()

        # Prepare the next page link
        next: Optional[str]
        if self.page_info.has_next_page:
            next = SkipCursorData(
                skip=skip + limit,
                limit=limit,
            ).encode()
        else:
            next = None

        # Done
        return PageLinks(prev=prev, next=next)

    def apply_to_statement(self, query: QueryObject, target_Model: SAModelOrAlias, stmt: sa.sql.Select) -> sa.sql.Select:
        # We will always load one more row to check if there's a next page
        skip = self.cursor_value.skip if self.cursor_value else 0
        limit = self.limit + 1 if self.limit is not None else None

        return stmt.offset(skip).limit(limit)

    def inspect_data_rows(self, query_executor: QueryExecutor, rows: list[SARowDict]):
        limit = self.limit

        # No limit? no inspection.
        if limit is None:
            self.page_info = SkipPageInfo(has_next_page=False)
            return

        # Do we have a next page?
        # If limit is set, we always load one more row to check if there's a next page
        expected_count = limit + 1
        has_next_page = len(rows) == expected_count

        # We've loaded one extra row. Now remove it.
        if has_next_page:
            rows.pop()

        # Done
        self.page_info = SkipPageInfo(has_next_page=has_next_page)


class SkipCursorData(NamedTuple):
    """ Cursor data for the "skip" cursor """
    skip: int
    limit: int

    def serialize(self) -> dict:
        # TODO: more compact names and encoding
        return {'skip': self.skip, 'limit': self.limit}

    def encode(self) -> str:
        return encode_opaque_cursor('skip', self.serialize())

    @classmethod
    def decode(cls, cursor: str):
        type, data = decode_opaque_cursor(cursor)
        return cls(**data)


@dataclass
class SkipPageInfo:
    """ Page info for the "skip" cursor """
    # Do we have any next page?
    has_next_page: bool
