import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

from jessiql.query_object import QueryObjectDict
from jessiql.testing.insert import insert
from jessiql.testing.recreate_tables import created_tables

from .util.models import IdManyFieldsMixin, id_manyfields
from .util.test_queries import typical_test_sql_query_text, typical_test_query_results, typical_test_query_text_and_results


@pytest.mark.parametrize(('query_object', 'expected_query_lines',), [
    # Empty: feed `None`
    (dict(skip=None), []),
    (dict(limit=None), []),
    # Values: skip 1, limit 1, skip 1 limit 1
    (dict(skip=1), ['LIMIT ALL OFFSET 1']),
    (dict(limit=1), ['LIMIT 1']),
    (dict(skip=1, limit=1), ['LIMIT 1 OFFSET 1']),
])
def test_skiplimit_sql(connection: sa.engine.Connection, query_object: QueryObjectDict, expected_query_lines: list[str]):
    """ Typical test: what SQL is generated """
    # Models
    Base = sa.orm.declarative_base()

    class Model(IdManyFieldsMixin, Base):
        __tablename__ = 'a'

        # This Postgres-specific implementation has .contains() and .overlaps() implementations
        tags = sa.Column(pg.ARRAY(sa.String))

    # Test
    typical_test_sql_query_text(query_object, Model, expected_query_lines)


@pytest.mark.parametrize(('query_object', 'expected_results'), [
    # Empty input
    (dict(), [{'id': n} for n in (1, 2, 3)]),
    # skip 1, limit 1, skip 1 limit 1
    # use `sort` to make sure the ordering is predictable
    (dict(sort=['id'], skip=1), [{'id': n} for n in (2, 3,)]),
    (dict(sort=['id'], limit=1), [{'id': n} for n in (1,)]),
    (dict(sort=['id'], skip=1, limit=1), [{'id': n} for n in (2,)]),
])
def test_skiplimit_results(connection: sa.engine.Connection, query_object: QueryObjectDict, expected_results: list[dict]):
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

        # Test
        typical_test_query_results(connection, query_object, Model, expected_results)


@pytest.mark.parametrize(('query_object', 'expected_query_lines', 'expected_results'), [
    (dict(select=[{'articles': dict(sort=['id'], limit=1)}]), [
        'SELECT anon_1.user_id, anon_1.id',
        'FROM (',
            'SELECT a.user_id AS user_id, a.id AS id, row_number() OVER (PARTITION BY a.user_id ORDER BY a.id ASC NULLS LAST) AS __group_row_n',
            'FROM a',
            'WHERE a.user_id IN ([POSTCOMPILE_primary_keys])',
            'ORDER BY a.id ASC NULLS LAST) AS anon_1',
        ')',
        'WHERE __group_row_n <= 1'
    ], [
        {'id': 1, 'articles': [
            {'id': 1, 'user_id': 1},
            # no more rows
        ]}
    ]),
    (dict(select=[{'articles': dict(sort=['id'], skip=1, limit=1)}]), ['WHERE __group_row_n > 1 AND __group_row_n <= 2'], [
         {'id': 1, 'articles': [
             # first row skipped
             {'id': 2, 'user_id': 1},
             # no more rows
         ]}
     ]),
    (dict(select=[{'articles': dict(sort=['id'], skip=1)}]), ['WHERE __group_row_n > 1'], [
        {'id': 1, 'articles': [
            # first row skipped
            {'id': 2, 'user_id': 1},
            {'id': 3, 'user_id': 1},
        ]}
    ]),
])
def test_joined_skiplimit(connection: sa.engine.Connection, query_object: QueryObjectDict, expected_query_lines: list[str], expected_results: list[dict]):
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

        # Test
        typical_test_query_text_and_results(connection, query_object, User, expected_query_lines, expected_results)
