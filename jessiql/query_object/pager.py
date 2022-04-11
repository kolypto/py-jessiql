
""" Query Object: pager operations """

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Union

from jessiql import exc

from .base import OperationInputBase


@dataclass
class SkipQuery(OperationInputBase):
    """ Query Object operation: the "skip" operation """
    # Skip: the number of objects to skip
    skip: Optional[int]

    @classmethod
    def from_query_object(cls, skip: Optional[int]):  # type: ignore[override]
        if skip is None or isinstance(skip, int):
            return cls(skip=skip)
        else:
            raise exc.QueryObjectError(f'"skip" must be an integer')

    def export(self) -> Optional[int]:
        return self.skip



@dataclass
class LimitQuery(OperationInputBase):
    """ Query Object operation: the "limit" operation """
    # Limit: the number of objects to limit the result set to
    limit: Optional[int]

    @classmethod
    def from_query_object(cls, limit: Optional[int]):  # type: ignore[override]
        if limit is None or isinstance(limit, int):
            return cls(limit=limit)
        else:
            raise exc.QueryObjectError(f'"limit" must be an integer')

    def export(self) -> Optional[int]:
        return self.limit



@dataclass
class BeforeQuery(OperationInputBase):
    """ Query Object operation: the "before" operation """
    # Cursor value for pagination
    cursor: Optional[str]

    @classmethod
    def from_query_object(cls, cursor: Optional[str]):  # type: ignore[override]
        return cls(cursor=cursor)

    def export(self) -> Optional[str]:
        return self.cursor


@dataclass
class AfterQuery(BeforeQuery):
    """ Query Object operation: the "after" operation """
