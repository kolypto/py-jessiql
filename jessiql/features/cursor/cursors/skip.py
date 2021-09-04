from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, NamedTuple

import sqlalchemy as sa

from jessiql.typing import SARowDict, SAModelOrAlias
from jessiql.engine.query import Query
from jessiql.query_object import QueryObject

from .base import CursorImplementation, PageLinks
from .encode import encode_opaque_cursor, decode_opaque_cursor


@dataclass
class SkipPageInfo:
    """ Page info for the "skip" cursor """
    # Do we have any next page?
    has_next_page: bool


class SkipCursorData(NamedTuple):
    """ Cursor data for the "skip" cursor """
    skip: int
    limit: int

    def serialize(self) -> dict:
        return {'skip': self.skip, 'limit': self.limit}

    def encode(self) -> str:
        return encode_opaque_cursor('skip', self.serialize())

    @classmethod
    def decode(cls, cursor: str):
        type, data = decode_opaque_cursor(cursor)
        return cls(**data)


class SkipCursor(CursorImplementation[SkipPageInfo, SkipCursorData]):
    """ Cursor implementation: "skip". Uses SKIP/LIMIT to paginate a query. """
    name = 'skip'
    PageInfo = SkipPageInfo
    CursorData = SkipCursorData

    @classmethod
    def pagination_possible(cls, query: QueryObject) -> bool:
        return True

    def generate_cursor_links(self, skip: int, limit: int) -> PageLinks:
        """ Generate links to next and previous page """
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

    @classmethod
    def apply_to_statement(cls, query: QueryObject, target_Model: SAModelOrAlias, stmt: sa.sql.Select, skip: int, limit: int, cursor: Optional[SkipCursorData]) -> sa.sql.Select:
        # We will always load one more row to check if there's a next page
        return stmt.offset(skip).limit(limit + 1)

    @classmethod
    def inspect_data_rows(cls, cursor_value: Optional[SkipCursorData], query_executor: Query, rows: list[SARowDict]) -> SkipPageInfo:
        # Decode cursor
        limit: Optional[int]
        if cursor_value is not None:
            limit = cursor_value.limit
        else:
            limit = query_executor.query.limit.limit

        # No limit? No inspection
        if limit is None:
            return SkipPageInfo(has_next_page=False)

        # Do we have a next page?
        # If limit is set, we always load one more row to check if there's a next page
        expected_count = limit + 1
        has_next_page = len(rows) == expected_count

        # We've loaded one extra row. Now remove it.
        if has_next_page:
            rows.pop()

        # Done
        return SkipPageInfo(has_next_page=has_next_page)
