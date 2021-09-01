from typing import Union

from .base import CursorImplementation, PageLinks
from .skip import SkipCursor, SkipCursorData, SkipPageInfo
from .keyset import KeysetCursor, KeysetCursorData, KeysetPageInfo


# A cursor data type
CursorData = Union[SkipCursorData, KeysetCursorData]

# A page info type
PageInfo = Union[SkipPageInfo, KeysetPageInfo]


def get_cursor_impl(cursor: str) -> type[CursorImplementation]:
    """ Given a cursor string, get a class that implements it, or fail """
    if cursor.startswith(SkipCursor.name):
        return SkipCursor
    elif cursor.startswith(KeysetCursor.name):
        return KeysetCursor
    else:
        raise NotImplementedError

