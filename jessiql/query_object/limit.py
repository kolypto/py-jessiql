""" Query Object: the "limit" operation """

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from jessiql import exc

from .base import OperationInputBase


@dataclass
class Limit(OperationInputBase):
    limit: Optional[int]

    @classmethod
    def from_query_object(cls, limit: Optional[int]):
        # Check types
        if limit is not None and not isinstance(limit, int):
            raise exc.QueryObjectError(f'"limit" must be an integer')

        # Construct
        return cls(limit=limit)
