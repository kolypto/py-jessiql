import sqlalchemy as sa

from jessiql.sautil.adapt import SimpleColumnsAdapter
from jessiql.typing import SAAttribute

from .base import Operation
from .sort import get_sort_fields_with_direction


class SkipLimitOperation(Operation):
    _window_over_foreign_keys: list[SAAttribute] = None

    def paginate_over_foreign_keys(self, fk_columns: list[SAAttribute]):
        self._window_over_foreign_keys = fk_columns

    def apply_to_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        if self._window_over_foreign_keys:
            return self._apply_window_over_foreign_key_pagination(stmt, fk_columns=self._window_over_foreign_keys)
        else:
            return self._apply_simple_skiplimit_pagination(stmt)

    def _apply_simple_skiplimit_pagination(self, stmt: sa.sql.Select):
        skip, limit = self.query.skip.skip, self.query.limit.limit

        if skip:
            stmt = stmt.offset(skip)
        if limit:
            stmt = stmt.limit(limit)

        # Done
        return stmt

    def _apply_window_over_foreign_key_pagination(self, stmt: sa.sql.Select, *, fk_columns: list[SAAttribute]) -> sa.sql.Select:
        """ Instead of the usual limit, use a window function over the given columns.

        This method is used with the selectin-load loading strategy to load a limited number of related
        items per every primary entity. Instead of using LIMIT, we will group rows over `fk_columns`,
        and impose a limit per group.

        This is achieved using a Window Function:

            SELECT *, row_number() OVER(PARTITION BY author_id) AS group_row_n
            FROM articles
            WHERE group_row_n < 10

            This will result in the following table:

            id  |   author_id   |   group_row_n
            ------------------------------------
            1       1               1
            2       1               2
            3       2               1
            4       2               2
            5       2               3
            6       3               1
            7       3               2
        """
        skip, limit = self.query.skip.skip, self.query.limit.limit

        # Apply it only when there's a limit
        if not skip and not limit:
            return stmt

        # First, add a row counter
        adapter = SimpleColumnsAdapter(self.target_Model)

        stmt = stmt.add_columns(
            sa.func.row_number().over(
                # Groups are partitioned by self._window_over_columns,
                partition_by=adapter.replace_many(fk_columns),
                # We have to apply the same ordering from the outside query;
                # otherwise, the numbering will be undetermined
                order_by=adapter.replace_many(
                    get_sort_fields_with_direction(self.query.sort, self.target_Model, where='limit')
                )
            )
            # give it a name that we can use later
            .label('__group_row_n')
        )

        # Wrap ourselves into a subquery.
        # This is necessary because Postgres does not let you reference SELECT aliases in the WHERE clause.
        # Reason: WHERE clause is executed before SELECT
        subquery = (
            # Taken from: Query.from_self()
            stmt
            .correlate(None)
            .subquery()
            ._anonymous_fromclause()
        )
        stmt = sa.select([
            column
            for column in subquery.c
            if column.key != '__group_row_n'  # skip this column. We don't need it.
        ]).select_from(subquery)

        # Apply the LIMIT condition using row numbers
        # These two statements simulate skip/limit using window functions
        if skip:
            stmt = stmt.filter(sa.sql.literal_column('__group_row_n') > skip)
        if limit:
            stmt = stmt.filter(sa.sql.literal_column('__group_row_n') <= ((skip or 0) + limit))

        # Done
        return stmt
