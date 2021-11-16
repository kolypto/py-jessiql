""" Convert a SA SQL statement to readable text """
import re
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

import jessiql.engine


# The dialect to use for compiling statements
DEFAULT_DIALECT: sa.engine.interfaces.Dialect = postgresql.dialect()  # type: ignore[misc]


def query2sql(query: jessiql.engine.QueryExecutor, dialect: sa.engine.interfaces.Dialect = None):
    """ Convert a JesiQL Query into a SQL string (for inspection) """
    return '\n\n\n'.join(
        stmt2sql(stmt, dialect)
        for stmt in query.all_statements()
    )


def stmt2sql(stmt: sa.sql.ClauseElement, dialect: sa.engine.interfaces.Dialect = None) -> str:
    """ Convert an SqlAlchemy statement into a string """
    # See: http://stackoverflow.com/a/4617623/134904
    # This intentionally does not escape values!
    query = stmt.compile(dialect=dialect or DEFAULT_DIALECT)
    return _insert_query_params(query.string, query.params)  # type: ignore[arg-type]


def selected_columns(stmt_str: str):
    """ Get the set of column names from the SELECT clause

        Example:
        SELECT a, u.b, c AS c_1, u.d AS u_d
        -> {'a', 'u.b', 'c', 'u.d'}
    """
    # Match
    m = SELECTED_COLUMNS_REX.match(stmt_str)

    # Results
    if not m:
        return set()

    selected_columns_str: str = m.group(1)

    # Match results
    results = EXTRACT_COLUMN_NAMES.findall(selected_columns_str)
    return frozenset(results)


# the whole SELECT <...> clause
SELECTED_COLUMNS_REX = re.compile(r'^SELECT (.*?)\s+FROM')

# extract column names, without 'as'
EXTRACT_COLUMN_NAMES = re.compile(r'(\S+?)(?: AS \w+)?(?:,|$)')


def _insert_query_params(statement_str: str, parameters: tuple):
    """ Compile a statement by inserting *unquoted* parameters into the query """
    return statement_str % parameters
