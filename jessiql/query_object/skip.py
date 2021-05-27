""" Query Object: the "skip" operation """

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from jessiql import exc

from .base import OperationInputBase


@dataclass
class Skip(OperationInputBase):
    skip: Optional[int]

    @classmethod
    def from_query_object(cls, skip: Optional[int]):
        # Check types
        if skip is not None and not isinstance(skip, int):
            raise exc.QueryObjectError(f'"skip" must be an integer')

        # Construct
        return cls(skip=skip)
