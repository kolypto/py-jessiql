from typing import Union

import sqlalchemy as sa
import sqlalchemy.orm

from jessiql.query import QueryExecutor
from jessiql.query_object import QueryObject, QueryObjectDict
from jessiql.typing import SARowDict


class JessiQL:
    Model: type
    query: QueryObject
    executor: QueryExecutor

    def __init__(self, Model: type, query_object: Union[QueryObjectDict, QueryObject]):
        self.Model = Model
        self.query = QueryObject.from_query_object(query_object)
        self.executor = QueryExecutor(self.query, Model)

    def execute(self, connection: sa.engine.Connection) -> list[SARowDict]:
        return self.executor.fetchall(connection)
