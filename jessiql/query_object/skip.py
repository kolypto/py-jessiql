""" Query Object: the "skip" operation """

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
    # Page: opaque cursor, used with cursor-based pagination
    page: Optional[str]

    @classmethod
    def from_query_object(cls, skip: Optional[Union[int, str]]):  # type: ignore[override]
        # Check types:
        if skip is None:
            return cls(skip=None, page=None)
        elif isinstance(skip, int):
            return cls(skip=skip, page=None)
        elif isinstance(skip, str) and skip.isdigit():
            return cls(skip=int(skip), page=None)
        elif isinstance(skip, str) and (skip.startswith('skip:') or skip.startswith('keys:')):
            return cls(skip=None, page=skip)
        else:
            raise exc.QueryObjectError(f'"skip" must be an integer or a cursor')

    def export(self) -> Optional[Union[int, str]]:
        return self.skip or self.page
