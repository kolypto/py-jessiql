import pytest
import sqlalchemy as sa

from jessiql import QueryObjectDict
from jessiql.testing.table_data import insert
from jessiql.testing.recreate_tables import created_tables
from jessiql.util import sacompat

from .util.models import IdManyFieldsMixin, id_manyfields
from .util.test_queries import typical_test_sql_query_text, typical_test_query_results, typical_test_query_text_and_results


@pytest.mark.parametrize(('query_object', 'expected_query_lines',), [
    # Empty
    (dict(sort=None), ['SELECT a.id', 'FROM a']),
    # Sort ASC, sort DESC
    (dict(sort=['a']), ['ORDER BY a.a ASC']),
    (dict(sort=['a+']), ['ORDER BY a.a ASC']),
    (dict(sort=['a-']), ['ORDER BY a.a DESC']),
    # Sort: many fields
    (dict(sort=['a', 'b+', 'c-']), ["ORDER BY a.a ASC NULLS LAST, a.b ASC NULLS LAST, a.c DESC NULLS LAST"]),
    # Sort: JSON
    (dict(sort=['j.user.id']), ["ORDER BY a.j #>> ('user', 'id') ASC"]),  # TODO: (tag:postgres-only) th]is is a PostgreSQL-specific expression
    # Sort: hybrid property
    (dict(sort=['awow']), ["ORDER BY a.a || ! ASC NULLS LAST"]),
    # Sort: related column
    (dict(sort=["related.a"]), ["SELECT a.id", "FROM a", "ORDER BY (SELECT r.a", "FROM r", "WHERE a.id = r.parent_id", "LIMIT 1) ASC NULLS LAST"]),
])
def test_sort_sql(connection: sa.engine.Connection, query_object: QueryObjectDict, expected_query_lines: list[str]):
    """ Typical test: what SQL is generated """
    # Models
    Base = sacompat.declarative_base()

    class Model(IdManyFieldsMixin, Base):
        __tablename__ = 'a'

        # A hybrid property can be used in expressions as well
        @sa.ext.hybrid.hybrid_property
        def awow(self): pass

        @awow.expression
        def awow(cls):
            return cls.a + '!'

        related = sa.orm.relationship('Related', back_populates='parent')

    class Related(IdManyFieldsMixin, Base):
        __tablename__ = 'r'

        parent_id = sa.Column(sa.ForeignKey(Model.id))
        parent = sa.orm.relationship(Model, back_populates='related')

    # Test
    typical_test_sql_query_text(query_object, Model, expected_query_lines)


@pytest.mark.parametrize(('query_object', 'expected_results'), [
    # Test: sort ASC, DESC
    (dict(sort=['id+']), [{'id': n} for n in (1, 2, 3)]),
    (dict(sort=['id-']), [{'id': n} for n in (3, 2, 1)]),
    # Test: sort JSON
    (dict(sort=['j.m-']), [{'id': n} for n in (3, 2, 1)]),
])
def test_sort_results(connection: sa.engine.Connection, query_object: QueryObjectDict, expected_results: list[dict]):
    """ Typical test: real data, real query, real results """
    # Models
    Base = sacompat.declarative_base()

    class Model(IdManyFieldsMixin, Base):
        __tablename__ = 'a'

    # Data
    with created_tables(connection, Base):
        # Insert some rows
        insert(connection, Model,
            id_manyfields('m', 1),
            id_manyfields('m', 2),
            id_manyfields('m', 3),
        )

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
    Base = sacompat.declarative_base()

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
        insert(connection, User,
            id_manyfields('u', 1),
        )
        insert(connection, Article,
            id_manyfields('a', 1, user_id=1),
            id_manyfields('a', 2, user_id=1),
            id_manyfields('a', 3, user_id=1),
        )

        # Test
        typical_test_query_text_and_results(connection, query_object, User, expected_query_lines, expected_results)
