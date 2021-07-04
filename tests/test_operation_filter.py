import pytest
import sqlalchemy as sa

from sqlalchemy.dialects import postgresql as pg

from jessiql.engine import JessiQL
from jessiql.query_object import QueryObjectDict
from jessiql.testing.insert import insert
from jessiql.testing.recreate_tables import created_tables
from jessiql.testing.stmt_text import assert_statement_lines, stmt2sql
from .util.models import IdManyFieldsMixin, id_manyfields


@pytest.mark.parametrize(('query_object', 'expected_query_lines',), [
    # Empty
    (dict(filter=None), []),
    (dict(filter={}), []),
    # Shortcut equality
    (dict(filter={'a': 1}), ["WHERE a.a = 1"]),
    # Scalar Operators
    (dict(filter={'a': {'$eq': 1}}), ["WHERE a.a = 1"]),
    (dict(filter={'a': {'$ne': 1}}), ["WHERE a.a IS DISTINCT FROM 1"]),
    (dict(filter={'a': {'$lt': 1}}), ["WHERE a.a < 1"]),
    (dict(filter={'a': {'$lte': 1}}), ["WHERE a.a <= 1"]),
    (dict(filter={'a': {'$gte': 1}}), ["WHERE a.a >= 1"]),
    (dict(filter={'a': {'$gt': 1}}), ["WHERE a.a > 1"]),
    (dict(filter={'a': {'$prefix': 'ex-'}}), ["WHERE (a.a LIKE ex- || '%')"]),
    (dict(filter={'a': {'$in': (1, 2, 3)}}), ["WHERE a.a IN ([POSTCOMPILE_a_1])"]),
    (dict(filter={'a': {'$nin': (1, 2, 3)}}), ["WHERE (a.a NOT IN ([POSTCOMPILE_a_1]))"]),
    (dict(filter={'a': {'$exists': 0}}), ["WHERE a.a IS NULL"]),
    (dict(filter={'a': {'$exists': 1}}), ["WHERE a.a IS NOT NULL"]),
    # Multiple scalar comparisons
    (dict(filter={'a': 1, 'b': 2}), ["WHERE a.a = 1 AND a.b = 2"]),
    (dict(filter={'a': {'$gt': 1, '$ne': 10}}), ["WHERE a.a > 1 AND a.a IS DISTINCT FROM 10"]),
    # Array operators, scalar operand
    (dict(filter={'tags': {'$eq': 'a'}}), ["WHERE a = ANY (a.tags)"]),
    (dict(filter={'tags': {'$ne': 'a'}}), ["WHERE a != ALL (a.tags)"]),
    (dict(filter={'tags': {'$exists': 1}}), ["WHERE a.tags IS NOT NULL"]),
    (dict(filter={'tags': {'$size': 0}}), ["WHERE array_length(a.tags, 1) IS NULL"]),
    (dict(filter={'tags': {'$size': 1}}), ["WHERE array_length(a.tags, 1) = 1"]),
    # Array operators, scalar operand
    (dict(filter={'tags': {'$eq': ['a', 'b', 'c']}}), ["WHERE a.tags = CAST(ARRAY[a, b, c] AS VARCHAR[])"]),
    (dict(filter={'tags': {'$ne': ['a', 'b', 'c']}}), ["WHERE a.tags != CAST(ARRAY[a, b, c] AS VARCHAR[])"]),
    (dict(filter={'tags': {'$in': ['a', 'b', 'c']}}), ["WHERE a.tags && CAST(ARRAY[a, b, c] AS VARCHAR[])"]),
    (dict(filter={'tags': {'$nin': ['a', 'b', 'c']}}), ["WHERE NOT a.tags && CAST(ARRAY[a, b, c] AS VARCHAR[])"]),
    (dict(filter={'tags': {'$all': ['a', 'b', 'c']}}), ["WHERE a.tags @> CAST(ARRAY[a, b, c] AS VARCHAR[])"]),
])
def test_filter_sql(connection: sa.engine.Connection, query_object: QueryObjectDict, expected_query_lines: list[str]):
    """ Typical test: what SQL is generated """
    # Models
    Base = sa.orm.declarative_base()

    class Model(IdManyFieldsMixin, Base):
        __tablename__ = 'a'

        # This Postgres-specific implementation has .contains() and .overlaps() implementations
        tags = sa.Column(pg.ARRAY(sa.String))

    # Query
    q = JessiQL(query_object, Model)

    # SQL
    assert assert_statement_lines(q.statement(), *expected_query_lines)


@pytest.mark.parametrize(('query_object', 'expected_results'), [
    # Empty input
    (dict(), [{'id': n} for n in (1, 2, 3)]),
    # Filter by column
    (dict(filter={'a': 'not-found'}), []),
    (dict(filter={'a': 'm-1-a'}), [{'id': 1}]),
])
def test_filter_results(connection: sa.engine.Connection, query_object: QueryObjectDict, expected_results: list[dict]):
    """ Typical test: real data, real query, real results """
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
        assert results == expected_results


@pytest.mark.parametrize(('query_object', 'expected_query_lines', 'expected_results'), [
    (dict(select=[{'articles': dict(filter={'id': 3})}]), [
        'FROM u',
        'FROM a',
        'WHERE a.user_id IN ([POSTCOMPILE_primary_keys]) AND a.id = 3'
    ], [
        {'id': 1, 'articles': [
            {'id': 3, 'user_id': 1},
            # no more rows
        ]}
    ]),
])
def test_joined_filter(connection: sa.engine.Connection, query_object: QueryObjectDict, expected_query_lines: list[str], expected_results: list[dict]):
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
        statements = '\n\n\n'.join(map(stmt2sql, q.all_statements()))
        assert assert_statement_lines(statements, *expected_query_lines)

        # Results
        results = q.fetchall(connection)
        assert results == expected_results
