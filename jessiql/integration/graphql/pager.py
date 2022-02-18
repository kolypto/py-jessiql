""" Pagination for GraphQL """

from __future__ import annotations

from typing import TypedDict, Optional

from jessiql.features.cursor import QueryPage


def pager_info(query: QueryPage) -> PagerInfoDict:
    """ Get pagination cursors """
    links = query.page_links()
    return {
        'prev': links.prev,
        'next': links.next,
    }


class PagerInfoDict(TypedDict):
    """ Pagination info object """
    prev: Optional[str]
    next: Optional[str]
