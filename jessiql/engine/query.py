""" Query: executes Query Objects against an SqlAlchemy Model class """

from functools import partial
from typing import Union

from jessiql.query_object import QueryObject, QueryObjectDict
from .query_executor import QueryExecutor


class Query(QueryExecutor):
    """ Executes Query Objects against a model.

    Example:
        query_object = dict(select=['login'])
        q = Query(query_object, models.User)
    """
    def __init__(self, query: Union[QueryObject, QueryObjectDict], Model: type):
        """ Prepare to make a query with this Query Object against target Model

        Args:
            query: The Query Object, parsed, or its dict
            Model: The Model class to query against

        Raises:
            exc.InvalidColumnError: Invalid column name mentioned (programming error)
            exc.InvalidRelationError: Invalid relation name mentioned (programming error)
            exc.QueryObjectError: Query object syntax error (wrong operator name, argument type)
        """
        # Parse the Query Object
        if not isinstance(query, QueryObject):
            query = QueryObject.from_query_object(query)

        # Proceed
        super().__init__(query, Model)

    @classmethod
    def prepare(cls, Model: type):
        """ Prepare to make a Query against the provided model

        Example:
            qUser = Query.prepare(models.User)
            q = qUser(query_object)
        """
        return partial(cls, Model=Model)
