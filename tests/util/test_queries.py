from jessiql.engine import JessiQL
from jessiql.query_object import QueryObjectDict
j
from jessiql.testing.stmt_text import assert_statement_lines, stmt2sql


def test_jessiql_sql_query_text(query_object: QueryObjectDict, Model: type, expected_query_lines: list[str]):
    # Query
    q = JessiQL(query_object, Model)

    # SQL
    assert assert_statement_lines(q.statement(), *expected_query_lines)
