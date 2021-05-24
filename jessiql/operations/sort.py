import sqlalchemy as sa

from jessiql.query_object import QueryObject, resolve_sorting_field_with_direction
from jessiql.typing import SAModelOrAlias


class SortOperation:
    def __init__(self, query: QueryObject, target_Model: SAModelOrAlias):
        self.query = query
        self.target_Model = target_Model

    def apply_to_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        # Sort fields
        stmt = stmt.order_by(*(
            resolve_sorting_field_with_direction(self.target_Model, field, where='sort')
            for field in self.query.sort.fields
        ))

        # Done
        return stmt
