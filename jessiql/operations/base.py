from __future__ import annotations

import sqlalchemy as sa
from typing import TYPE_CHECKING

from jessiql.query_object import QueryObject
from jessiql.typing import SAModelOrAlias


if TYPE_CHECKING:
    from jessiql.engine.query_executor import QueryExecutor
    from jessiql.engine.settings import QuerySettings


class Operation:
    """ Base for all operations. Defines the interface """
    query: QueryObject
    target_Model: SAModelOrAlias
    settings: QuerySettings

    def __init__(self, query: QueryObject, target_Model: SAModelOrAlias, settings: QuerySettings):
        self.query = query
        self.target_Model = target_Model
        self.settings = settings

    def for_query(self, query_executor: QueryExecutor):
        """ Bind this operation to a QueryExecutor

        Called immediately after __init__().
        Make sure that you don't keep a strong reference to `query_executor` because that would be a cyclic dependency
        and you'll have a memory leak!
        """
        return self

    __slots__ = 'query', 'target_Model', 'settings'

    def apply_to_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        """ Modify the SQL Select statement that produces resulting rows """
        raise NotImplementedError

    def apply_to_results(self, query_executor: QueryExecutor, rows: list[dict]) -> list[dict]:
        """ Customize the resulting rows """
        return rows
