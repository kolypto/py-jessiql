from functools import partial

from typing import Union

from jessiql.query_object import QueryObject, QueryObjectDict

from .query_executor import QueryExecutor


class Query(QueryExecutor):
    def __init__(self, query: Union[QueryObject, QueryObjectDict], target_Model: type):
        if not isinstance(query, QueryObject):
            query = QueryObject.from_query_object(query)

        super().__init__(query, target_Model)

    @classmethod
    def prepare(cls, target_Model: type):
        return partial(cls, target_Model=target_Model)
