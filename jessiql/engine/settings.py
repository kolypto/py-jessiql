from __future__ import annotations

import dataclasses
from collections import abc
from typing import Optional, Union, TYPE_CHECKING


if TYPE_CHECKING:
    import sqlalchemy as sa
    from .query_executor import QueryExecutor, SARowDict
    from jessiql.query_object.rewrite.rewrite import Rewriter


@dataclasses.dataclass
class QuerySettings:
    """ Settings for Query

    This object defines additional behavior that may be used with queries:
    limit result rows, customize queries, configure related queries, etc
    """
    # The `limit` you get by default, if not specified
    default_limit: Optional[int] = None

    # The max number of items you get, regardless of the limit
    max_limit: Optional[int] = None

    # Field names rewriter
    rewriter: Optional[Rewriter] = None

    # Settings for nested queries: i.e. relations
    # A mapping { relation name => Query Settings}, where the value can optionally be a lambda
    relations: Optional[dict[str, Union[QuerySettings, abc.Callable[[], QuerySettings]]]] = None

    # Getter function for relation settings
    relation_settings_getter: Optional[abc.Callable[[str], Optional[QuerySettings]]] = None

    def __post_init__(self):
        # Associate this rewriter with a QuerySettings
        if self.rewriter:
            # Make sure that it's 1-1 link. Why? Because every QuerySettings links to exactly one Rewriter, and they know one another.
            assert self.rewriter.settings is None, 'Sorry, you cannot associate the same rewriter with multiple QuerySettings. Make a copy().'

            # Ok? associate.
            self.rewriter.settings = self

    # ### Callbacks for QueryExecutor
    # QueryExecutor and Operations will use these methods to apply the settings

    def get_final_limit(self, limit: Optional[int]) -> Optional[int]:
        """ Callback that fine-tunes the `limit` of a query by applying default and max limits

        Used by: the "limit" operation to decide how many rows to limit the result set to.
        """
        # Apply default limit
        if not limit:
            limit = self.default_limit

        # Apply min limit
        if limit and self.max_limit:
            limit = min(limit, self.max_limit)

        # Done
        return limit

    def get_relation_settings(self, relation_name: str) -> Optional[QuerySettings]:
        """ Callback that returns query settings for a nested relationship

        Used by: QueryExecutor to get QuerySettings for related queries (i.e. those made through "join" query operation)

        Default behavior: use `relation_settings_getter(name)`, fall back to `self.relations[name]`
        You can override this method for custom behavior
        """
        return (
            # Use the getter function, if provided
            (self.relation_settings_getter and self.relation_settings_getter(relation_name)) or
            # Fall back to the dict key, if available
            _getitem_callable(self.relations, relation_name) or
            # Give None, if nothing worked
            None
        )

    def get_db_field_name(self, api_name: str) -> str:
        """ Callback: convert API field name (e.g. camelCase) to DB field name (e.g. snake_case) """
        return api_name

    def get_api_field_name(self, db_name: str) -> str:
        """ Callback: convert DB field name (e.g. snake_case) to API field name (e.g. camelCase) """
        return db_name

    # TODO: implement virtual fields

    def customize_statement(self, query: QueryExecutor, stmt: sa.sql.Select) -> sa.sql.Select:
        """ Callback that customizes query statement

        Used by: QueryExecutor to customize the statement after all but "limit" operations were applied.
        This particular function is applied both to the top-level query and to nested queries as well

        Default behavior: none
        You can override this method for custom behavior
        """
        # TODO: implement nested customization handler that uses nested settings and invokes customization without `path` complexity
        return stmt

    def customize_result(self, query: QueryExecutor, rows: list[SARowDict]) -> list[SARowDict]:
        """ Callback that customizes query results

        Used by: QueryExecutor to customize result rows right before they are returned to the user
        This particular function is applied both to the top-level rows and to nested results as well

        Default behavior: none
        You can override this method for custom behavior
        """
        return rows


def _getitem_callable(d: Optional[dict], k: str):
    """ Get d[k] if possible. Resolve the value if its callable """
    if d is None:
        return None

    value = d.get(k)
    if callable(value):
        value = value()

    return value
