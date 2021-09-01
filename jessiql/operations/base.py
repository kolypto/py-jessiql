import sqlalchemy as sa

from jessiql.query_object import QueryObject
from jessiql.typing import SAModelOrAlias


class Operation:
    """ Base for all operations. Defines the interface """
    query: QueryObject
    target_Model: SAModelOrAlias

    def __init__(self, query: QueryObject, target_Model: SAModelOrAlias):
        self.query = query
        self.target_Model = target_Model

    __slots__ = 'query', 'target_Model'

    def apply_to_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        """ Modify the SQL Select statement that produces resulting rows """
        raise NotImplementedError
