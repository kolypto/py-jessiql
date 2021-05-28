import sqlalchemy as sa

from jessiql.query_object.resolve import resolve_sorting_field_with_direction

from .base import Operation


class SortOperation(Operation):
    def apply_to_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        # Sort fields
        stmt = stmt.order_by(
            *self.compile_columns()
        )

        # Done
        return stmt

    def compile_columns(self) -> list[sa.sql.ColumnElement]:
        return [
            resolve_sorting_field_with_direction(field, self.target_Model, where='sort')
            for field in self.query.sort.fields
        ]
