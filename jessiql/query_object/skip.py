""" Query Object: the "skip" operation """

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from jessiql import exc

from .base import OperationInputBase


@dataclass
class SkipQuery(OperationInputBase):
    """ Query Object operation: the "skip" operation """
    # Skip: the number of objects to skip
    skip: Optional[int]

    @classmethod
    def from_query_object(cls, skip: Optional[int]):
        # Check types
        if skip is not None and not isinstance(skip, int):
            raise exc.QueryObjectError(f'"skip" must be an integer')

        # Construct
        return cls(skip=skip)

    def export(self) -> Optional[int]:
        return self.skip
