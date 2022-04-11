from __future__ import annotations

from typing import Optional, TYPE_CHECKING

import sqlalchemy as sa

from jessiql.typing import SAAttribute, SAModelOrAlias
from jessiql.query_object import QueryObject
from jessiql.sautil.adapt import SimpleColumnsAdapter
from jessiql.sainfo.version import SA_14

from jessiql.operations.base import Operation
from jessiql.operations.sort import get_sort_fields_with_direction
from jessiql.util.sacompat import add_columns


if TYPE_CHECKING:
    from jessiql.engine.settings import QuerySettings
    from .page_links import PageLinks


class SkipLimitOperation(Operation):
    """ Skip/Limit operation: pagination

    Handles: QueryObject.skip, QueryObject.limit
    When applied to a statement:
    * Adds SKIP/LIMIT clauses

    In paginate_over_foreign_keys() mode, uses a window function over a set of foreign keys:
    this way every related object gets its own pagination!
    """

    skip: Optional[int]
    limit: Optional[int]

    def __init__(self, query: QueryObject, target_Model: SAModelOrAlias, settings: QuerySettings):
        super().__init__(query, target_Model, settings)
        self._window_over_foreign_keys = None

        # Prepare the values in advance
        # Why? because subclasses may want to modify it.
        self.skip = self.query.skip.skip
        self.limit = self.settings.get_final_limit(self.query.limit.limit)

    __slots__ = '_window_over_foreign_keys', 'skip', 'limit'

    # Enables pagination with a window function.
    # Value: list of foreign keys attributes to iterate against
    _window_over_foreign_keys: Optional[list[SAAttribute]]

    def get_page_links(self) -> PageLinks:
        raise NotImplementedError('Cursors are not supported for related objects')

    def paginate_over_foreign_keys(self, fk_columns: list[SAAttribute]):
        """ Enable pagination over foreign keys

        This is used for paginating related objects which are loaded with one query:
        every parent object gets its own pagination window.

        See: self._apply_window_over_foreign_key_pagination()
        """
        self._window_over_foreign_keys = fk_columns

    def apply_to_statement(self, stmt: sa.sql.Select) -> sa.sql.Select:
        """ Modify the Select statement: add SKIP/LIMIT clauses or PARTITION BY clause """
        # When in window-function mode
        if self._window_over_foreign_keys:
            return self._apply_window_over_foreign_key_pagination(stmt, fk_columns=self._window_over_foreign_keys)
        # When in SKIP/LIMIT mode
        else:
            return self._apply_simple_skiplimit_pagination(stmt)

    def _apply_simple_skiplimit_pagination(self, stmt: sa.sql.Select):
        """ Pagination for the SKIP/LIMIT mode: add SKIP/LIMIT clauses """
        if self.skip:
            stmt = stmt.offset(self.skip)
        if self.limit:
            stmt = stmt.limit(self.limit)

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
        skip, limit = self.skip, self.limit

        # Apply it only when there's a limit
        if not skip and not limit:
            return stmt

        # First, add a row counter
        adapter = SimpleColumnsAdapter(self.target_Model)
        row_counter_col = (
            sa.func.row_number().over(
                # Groups are partitioned by self._window_over_columns,
                partition_by=adapter.replace_many(fk_columns),  # type: ignore[arg-type]
                # We have to apply the same ordering from the outside query;
                # otherwise, the numbering will be undetermined
                order_by=adapter.replace_many(
                    get_sort_fields_with_direction(self.query.sort, self.target_Model, where='limit')
                )  # type: ignore[arg-type]
            )
            # give it a name that we can use later
            .label('__group_row_n')
        )
        stmt = add_columns(stmt, [row_counter_col])

        # Wrap ourselves into a subquery.
        # This is necessary because Postgres does not let you reference SELECT aliases in the WHERE clause.
        # Reason: WHERE clause is executed before SELECT
        if SA_14:
            subquery = (
                # Taken from: Query.from_self()
                stmt
                .correlate(None)
                .subquery()
                ._anonymous_fromclause()  # type: ignore[attr-defined]
            )
        else:
            subquery = (
                stmt
                .correlate(None)
                .alias()
            )

        stmt = sa.select([
            column
            for column in subquery.c
            if column.key != '__group_row_n'  # skip this column. We don't need it.
        ]).select_from(subquery)

        # Apply the LIMIT condition using row numbers
        # These two statements simulate skip/limit using window functions
        if skip:
            if SA_14:
                stmt = stmt.filter(sa.sql.literal_column('__group_row_n') > skip)
            else:
                stmt = stmt.where(sa.sql.literal_column('__group_row_n') > skip)
        if limit:
            if SA_14:
                stmt = stmt.filter(sa.sql.literal_column('__group_row_n') <= ((skip or 0) + limit))
            else:
                stmt = stmt.where(sa.sql.literal_column('__group_row_n') <= ((skip or 0) + limit))

        # Done
        return stmt
