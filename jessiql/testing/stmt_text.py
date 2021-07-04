""" Get a statement text """
import re
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# The dialect to use for compiling statements
DEFAULT_DIALECT: sa.engine.interfaces.Dialect = postgresql.dialect()


def stmt2sql(stmt: sa.sql.ClauseElement, dialect: sa.engine.interfaces.Dialect = None) -> str:
    """ Convert an SqlAlchemy statement into a string """
    # See: http://stackoverflow.com/a/4617623/134904
    # This intentionally does not escape values!
    query = stmt.compile(dialect=dialect or DEFAULT_DIALECT)
    return _insert_query_params(query.string, query.params)


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

    selected_columns_str = m.group(1)

    # Match results
    m = EXTRACT_COLUMN_NAMES.findall(selected_columns_str)
    return frozenset(m)


# the whole SELECT <...> clause
SELECTED_COLUMNS_REX = re.compile(r'^SELECT (.*?)\s+FROM')
# extract column names, without 'as'
EXTRACT_COLUMN_NAMES = re.compile(r'(\S+?)(?: AS \w+)?(?:,|$)')


def _insert_query_params(statement_str: str, parameters: tuple):
    """ Compile a statement by inserting *unquoted* parameters into the query """
    return statement_str % parameters
