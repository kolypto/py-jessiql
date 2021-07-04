import sqlalchemy as sa

from jessiql.engine import Query
from jessiql.query_object import QueryObjectDict

from jessiql.testing.stmt_text import assert_statement_lines, stmt2sql


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
    statements = '\n\n\n'.join(map(stmt2sql, q.all_statements()))
    assert assert_statement_lines(statements, *expected_query_lines)

    # Results
    results = q.fetchall(connection)
    assert results == expected_results
