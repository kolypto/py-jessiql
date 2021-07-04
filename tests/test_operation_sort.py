import pytest
import sqlalchemy as sa

from jessiql import QueryObjectDict
from jessiql.testing.insert import insert
from jessiql.testing.recreate_tables import created_tables

from .util.models import IdManyFieldsMixin, id_manyfields
from .util.test_queries import typical_test_sql_query_text, typical_test_query_results, typical_test_query_text_and_results


@pytest.mark.parametrize(('query_object', 'expected_query_lines',), [
    # Empty
    (dict(sort=None), ['SELECT a.id', 'FROM a']),
    # Sort ASC, sort DESC
    (dict(sort=['a']), 'ORDER BY a.a ASC'),
    (dict(sort=['a+']), 'ORDER BY a.a ASC'),
    (dict(sort=['a-']), 'ORDER BY a.a DESC'),
    # Sort: many fields
    (dict(sort=['a', 'b+', 'c-']), 'ORDER BY a.a ASC, a.b ASC, a.c DESC'),
])
def test_sort_sql(connection: sa.engine.Connection, query_object: QueryObjectDict, expected_query_lines: list[str]):
    """ Typical test: what SQL is generated """
    # Models
    Base = sa.orm.declarative_base()

    class Model(IdManyFieldsMixin, Base):
        __tablename__ = 'a'

    # Test
    typical_test_sql_query_text(query_object, Model, expected_query_lines)


@pytest.mark.parametrize(('query_object', 'expected_results'), [
    # Test: sort ASC, DESC
    (dict(sort=['id+']), [{'id': n} for n in (1, 2, 3)]),
    (dict(sort=['id-']), [{'id': n} for n in (3, 2, 1)]),
])
def test_sort_results(connection: sa.engine.Connection, query_object: QueryObjectDict, expected_results: list[dict]):
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
    # Simple sort: column
    (dict(sort=['a-'], select=[{'articles': dict(sort=['a-'])}]), [
        'FROM u',
        'ORDER BY u.a DESC',
        'FROM a',
        'ORDER BY a.a DESC'
    ], [
        {'id': 1, 'articles': [
            {'id': 3, 'user_id': 1},
            {'id': 2, 'user_id': 1},
            {'id': 1, 'user_id': 1},
        ]}
    ]),
])
def test_joined_sort(connection: sa.engine.Connection, query_object: QueryObjectDict, expected_query_lines: list[str], expected_results: list[dict]):
    """ Typical test: JOINs, SQL and results """
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

