""" Query Object: the "limit" operation """

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from jessiql import exc

from .base import OperationInputBase


@dataclass
class LimitQuery(OperationInputBase):
    """ Query Object operation: the "limit" operation """
    # Limit: the number of objects to limit the result set to
    limit: Optional[int]

    @classmethod
    def from_query_object(cls, limit: Optional[int]):
        # Check types
        if limit is not None and not isinstance(limit, int):
            raise exc.QueryObjectError(f'"limit" must be an integer')

        # Construct
        return cls(limit=limit)

    def export(self) -> Optional[int]:
        return self.limit
