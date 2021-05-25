from collections import abc

import sqlalchemy as sa

from jessiql.query_object import QueryObject
from jessiql.typing import SAModelOrAlias


class Operation:
    query: QueryObject
    target_Model: SAModelOrAlias

    def __init__(self, query: QueryObject, target_Model: SAModelOrAlias):
        self.query = query
        self.target_Model = target_Model

    def apply_to_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        raise NotImplementedError
