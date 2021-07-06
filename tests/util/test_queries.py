import sqlalchemy as sa
from typing import Union

from jessiql.engine import Query
from jessiql.query_object import QueryObjectDict

from jessiql.testing.stmt_text import stmt2sql, selected_columns


def typical_test_sql_query_text(query_object: QueryObjectDict, Model: type, expected_query_lines: list[str]):
    """ Typical test helper: make a query, check SQL

    Typical test scenario:
    * Take a Query Object
    * Create a query
    * Check SQL
    """
    # Query
    q = Query(query_object, Model)

    # SQL
    assert assert_statement_lines(q.statement(), *expected_query_lines)


def typical_test_sql_selected_columns(query_object: QueryObjectDict, Model: type, expected_columns: list[str]):
    """ Typical test helper: make a query, check SQL selected columns """
    # Query
    q = Query(query_object, Model)

    # SQL
    assert assert_selected_columns(q.statement(), *expected_columns)


def typical_test_query_results(connection: sa.engine.Connection, query_object: QueryObjectDict, Model: type, expected_results: list[dict]):
    """ Typical test helper: execute a query, check fetched rows

    Typical test scenario:
    * Take a Query Object
    * Create a query
    * Execute
    * Check fetched rows
    """
    # Query
    q = Query(query_object, Model)

    # Results
    results = q.fetchall(connection)
    assert results == expected_results


def typical_test_query_text_and_results(connection: sa.engine.Connection, query_object: QueryObjectDict, Model: type, expected_query_lines: list[str], expected_results: list[dict]):
    """ Typical test helper: check SQL, check fetched rows """
    # Query
    q = Query(query_object, Model)

    # SQL
    assert_query_statements_lines(q, *expected_query_lines)

    # Results
    results = q.fetchall(connection)
    assert results == expected_results


def assert_query_statements_lines(query: Query, *expected_lines: str, dialect: sa.engine.interfaces.Dialect = None):
    """ Render a Query and check that the provided lines are in there """
    statements = '\n\n\n'.join(map(stmt2sql, query.all_statements()))
    assert_statement_lines(statements, *expected_lines, dialect=dialect)


def assert_statement_lines(stmt: Union[str, sa.sql.ClauseElement], *expected_lines: str, dialect: sa.engine.interfaces.Dialect = None):
    """ Find the provided lines inside a statement or fail """
    # Query?
    if isinstance(stmt, sa.sql.ClauseElement):
        stmt = stmt2sql(stmt, dialect)

    # Test
    for line in expected_lines:
        assert line.strip() in stmt, f'{line!r} not found in {stmt!r}'

    # Done
    return stmt


def assert_selected_columns(stmt: Union[str, sa.sql.ClauseElement], *expected_columns: str, dialect: sa.engine.interfaces.Dialect = None):
    """ Match the two lists of selected columns or fail """
    # Query?
    if isinstance(stmt, sa.sql.ClauseElement):
        stmt = stmt2sql(stmt, dialect)

    # Test
    actual_columns = set(selected_columns(stmt))
    assert set(expected_columns) == set(actual_columns)

    # Done
    return stmt

