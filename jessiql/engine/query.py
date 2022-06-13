""" Query: executes Query Objects against an SqlAlchemy Model class """

from __future__ import annotations

from functools import partial
from typing import Optional, Union, TYPE_CHECKING

from jessiql.query_object import QueryObject, QueryObjectDict
from .query_executor import QueryExecutor, QuerySettings


if TYPE_CHECKING:
    from jessiql.operations.pager.page_links import PageLinks


class Query(QueryExecutor):
    """ JessiQL Query: executes query objects against a model.

    This class contains shortcuts, helpers, and sugar -- in addition to what QueryExecutor does.

    Example:
        query_object = {'select': ['login']}
        q = Query(query_object, models.User)
    """
    def __init__(self, query: Union[QueryObject, QueryObjectDict], Model: type, settings: QuerySettings = None):
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
        query = QueryObject.ensure_query_object(query)

        # Proceed
        super().__init__(query, Model, settings=settings)

    @classmethod
    def prepare(cls, Model: type, settings: QuerySettings = None):
        """ Prepare to make a Query against the provided model

        Example:
            user_settings = jessiql.QuerySettings(...)
            query_user = Query.prepare(models.User, user_settings)
            q = query_user(query_object)
        """
        return partial(cls, Model=Model, settings=settings)

    @property
    def query_level(self) -> int:
        """ Get the "level" of this query

        Level 0: the root query
        Level 1: query that loads related objects
        Level 2: query that loads related objects of the next level
        """
        # First level: (Model,)
        # Second level: (Model, relation name, relationship property)
        return (len(self.load_path) - 1) // 2

    @property
    def limit(self) -> Optional[int]:
        """ Get the final LIMIT set on the query

        It may be changed because of:
        1. User query
        2. Default limit
        3. Max limit
        """
        return self.pager_op.limit

    def page_links(self) -> PageLinks:
        """ Get links to the previous and next page

        These values are opaque cursors that you can feed to "before" or "after" to get to the corresponding page
        """
        return self.pager_op.get_page_links()
