""" Get a statement text """
import re
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from typing import Union

# The dialect to use for compiling statements
DEFAULT_DIALECT: sa.engine.interfaces.Dialect = postgresql.dialect()


def stmt2sql(stmt: sa.sql.ClauseElement, dialect: sa.engine.interfaces.Dialect = None) -> str:
    """ Convert an SqlAlchemy statement into a string """
    # See: http://stackoverflow.com/a/4617623/134904
    # This intentionally does not escape values!
    query = stmt.compile(dialect=dialect or DEFAULT_DIALECT)
    return _insert_query_params(query.string, query.params)


def assert_statement_lines(stmt: Union[str, sa.sql.ClauseElement], *expected_lines: str):
    """ Find the provided lines inside a statement or fail """
    # Query?
    if isinstance(stmt, sa.sql.ClauseElement):
        stmt = stmt2sql(stmt)

    # Test
    for line in expected_lines:
        assert line.strip() in stmt, f'{line!r} not found in {stmt!r}'

    # Done
    return stmt


def selected_columns(stmt: str):
    """ Get the set of column names from the SELECT clause

        Example:
        SELECT a, u.b, c AS c_1, u.d AS u_d
        -> {'a', 'u.b', 'c', 'u.d'}
    """
    # Match
    m = SELECTED_COLUMNS_REX.match(stmt)

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
