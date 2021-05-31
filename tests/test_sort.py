import pytest
import sqlalchemy as sa

from jessiql.engine import JessiQL
from jessiql.query_object import QueryObjectDict
from jessiql.testing.insert import insert
from jessiql.testing.recreate_tables import created_tables
from jessiql.testing.stmt_text import assert_statement_lines, stmt2sql
from .util.models import IdManyFieldsMixin, id_manyfields


@pytest.mark.parametrize(('query_object', 'expected_query_lines',), [
    (dict(sort=None), ['SELECT a.id', 'FROM a']),
    (dict(sort=['a']), 'ORDER BY a.a ASC'),
    (dict(sort=['a+']), 'ORDER BY a.a ASC'),
    (dict(sort=['a-']), 'ORDER BY a.a DESC'),
    (dict(sort=['a', 'b+', 'c-']), 'ORDER BY a.a ASC, a.b ASC, a.c DESC'),
])
def test_sort_sql(connection: sa.engine.Connection, query_object: QueryObjectDict, expected_query_lines: list[str]):
    # Models
    Base = sa.orm.declarative_base()

    class Model(IdManyFieldsMixin, Base):
        __tablename__ = 'a'

    # Query
    q = JessiQL(query_object, Model)

    # SQL
    assert assert_statement_lines(q.statement(), *expected_query_lines)


@pytest.mark.parametrize(('query_object', 'expected_ids'), [
    (dict(sort=['id+']), [1, 2, 3]),
    (dict(sort=['id-']), [3, 2, 1]),
])
def test_sort_results(connection: sa.engine.Connection, query_object: QueryObjectDict, expected_ids: list[dict]):
    # Models
    Base = sa.orm.declarative_base()

    class Model(IdManyFieldsMixin, Base):
        __tablename__ = 'a'

    # Data
    with created_tables(connection, Base):
        # Insert some rows
        insert(connection, Model, [
            id_manyfields('m', 1),
            id_manyfields('m', 2),
            id_manyfields('m', 3),
        ])

        # Query
        q = JessiQL(query_object, Model)

        # Results
        results = q.fetchall(connection)
        assert [row['id'] for row in results] == expected_ids


@pytest.mark.parametrize(('query_object', 'expected_query_lines', 'expected_results'), [
    (dict(), [], []),
])
def test_sort_sql(connection: sa.engine.Connection, query_object: QueryObjectDict, expected_query_lines: list[str], expected_results: list[dict]):
    # Models
    Base = sa.orm.declarative_base()

    class User(IdManyFieldsMixin, Base):
        __tablename__ = 'u'

        articles = sa.orm.relationship('Article', back_populates='author')

    class Article(IdManyFieldsMixin, Base):
        __tablename__ = 'a'

        user_id = sa.Column(sa.ForeignKey(User.id))
        author = sa.orm.relationship(User, back_populates='articles')

    # Data
    with created_tables(connection, Base):
        # Insert some rows
        insert(connection, User, [
            id_manyfields('u', 1),
        ])
        insert(connection, Article, [
            id_manyfields('a', 1, user_id=1),
            id_manyfields('a', 2, user_id=1),
            id_manyfields('a', 3, user_id=1),
        ])

        # Query
        q = JessiQL(query_object, User)

        # SQL
        statements = '\n'.join(map(stmt2sql, [
            q.statement(),
            # q.related_executors['article'].statement(),
        ]))
        assert assert_statement_lines(q.statement(), *expected_query_lines)

        # Results
        results = q.fetchall(connection)
        assert results == expected_results