import pytest
import sqlalchemy as sa

from jessiql import QueryObjectDict
from jessiql.testing.insert import insert
from jessiql.testing.recreate_tables import created_tables

from .util.models import IdManyFieldsMixin, id_manyfields
from .util.test_queries import typical_test_sql_selected_columns, typical_test_query_results, typical_test_query_text_and_results
from .util import okok


@pytest.mark.parametrize(('query_object', 'expected_columns',), [
    # Empty: feed `None`
    (dict(select=None), ['a.id']),  # NOTE: primary key is included by default
    # Select columns
    (dict(select=['id']), ['a.id']),
    (dict(select=['a']), ['a.a']),  # NOTE: primary key not selected, not included
])
def test_select_sql(connection: sa.engine.Connection, query_object: QueryObjectDict, expected_columns: list[str]):
    """ Typical test: what SQL is generated """
    # Models
    Base = sa.orm.declarative_base()

    class Model(IdManyFieldsMixin, Base):
        __tablename__ = 'a'

    # Test
    typical_test_sql_selected_columns(query_object, Model, expected_columns)


@pytest.mark.parametrize(('query_object', 'expected_results'), [
    # Empty input
    (dict(), [
        # PK is always selected
        {'id': 1},
    ]),
    # Select columns
    (dict(select=['a']), [
        {'a': 'm-1-a'},
    ]),
])
def test_select_results(connection: sa.engine.Connection, query_object: QueryObjectDict, expected_results: list[dict]):
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
        ])

        # Test
        typical_test_query_results(connection, query_object, Model, expected_results)


# NOTE: Below goes the most extensive test for related queries


@pytest.mark.parametrize(('model', 'query_object', 'expected_query_lines', 'expected_results'), [
    # === Test: One-To-Many, FK on remote side
    # User.articles: One-To-Many, FK on remote side
    # Joined query: nothing specified
    ('User', dict(select=[
        # Main: select <default>
        # Join: select <default>
        {'articles': dict()}  # nothing specific
    ]), [
        # Default: selected the PK
        'SELECT u.id',
        # Joined: selects the FK anyway, selects the PK by default
        'SELECT a.user_id, a.id',
        # Joined: join condition works
        'WHERE a.user_id IN ([POSTCOMPILE_primary_keys])',
    ], [
        # Three users, articles correctly distributed
        {'id': 1, 'articles': [
            {'user_id': 1, 'id': 1},
            {'user_id': 1, 'id': 2},
        ]},
        {'id': 2, 'articles': [
            {'user_id': 2, 'id': 3},
        ]},
        {'id': 3, 'articles': [
            {'user_id': 3, 'id': 4},
        ]},
    ]),
    # User.articles: 1-N, FK on remote side
    # Joined query: select related column
    ('User', dict(select=[
        # Main: select my column
        'a',
        # Join: select related column
        {'articles': dict(select=['a'])}
    ]), [
        # Main: select my column
        # NOTE: PK is still selected (for join)
        'SELECT u.a, u.id',
        # Join: selects my column
        # NOTE: PK is not selected anymore
        # NOTE: FK selected for join
        'SELECT a.user_id, a.a',
        # Joined: join condition works
        'WHERE a.user_id IN ([POSTCOMPILE_primary_keys])',
    ], [
        # Main: 'a' selected, 'id' still selected
        {'id': 1, 'a': 'u-1-a', 'articles': [
            # Joined: 'a' selected
            {'user_id': 1, 'a': 'a-1-a'},
            {'user_id': 1, 'a': 'a-2-a'},
        ]},
        okok.Whatever,
        okok.Whatever,
    ]),
    # === Test: Many-To-One, FK on local side
    ('Article', dict(select=[
        # Join: select related column
        {'author': dict(select=['a'])},
    ]), [
        # Main: does not select PK (because there is one column selected already)
        # NOTE: FK selected for join
        'SELECT a.user_id \n',
        # Join: selects my column
        # NOTE: PK selected for join
        'SELECT u.id, u.a',
        'WHERE u.id IN ([POSTCOMPILE_primary_keys])',
    ], [
        # Author selected for every user
        # Value: dict, not an array
        {'user_id': 1, 'author': {'id': 1, 'a': 'u-1-a'}},
        {'user_id': 1, 'author': {'id': 1, 'a': 'u-1-a'}},
        {'user_id': 2, 'author': {'id': 2, 'a': 'u-2-a'}},
        {'user_id': 3, 'author': {'id': 3, 'a': 'u-3-a'}},
        # No author: `None`
        {'user_id': None, 'author': None},
    ]),
    # === Test: three-level JOIN
    ('User', dict(select=[
        {'articles': dict(select=[
            {'comments': dict(select=[
                {'author': dict()}
            ])}
        ])}
    ]), [
        # Main
        'SELECT u.id',
        'FROM u',
        # Join: articles
        'SELECT a.user_id, a.id',
        'FROM a',
        'WHERE a.user_id IN ([POSTCOMPILE_primary_keys])',
        # Join: comments
        'SELECT c.article_id, c.user_id',
        'FROM c',
        'WHERE c.article_id IN ([POSTCOMPILE_primary_keys])',
        # Join: author
        'SELECT u.id',
        'FROM u',
        'WHERE u.id IN ([POSTCOMPILE_primary_keys])',
    ], [
         # Main
         {'id': 1, 'articles': [
             # articles
             {'id': 1, 'user_id': 1, 'comments': [
                 # comments, author
                 {'article_id': 1, 'user_id': 1, 'author': {'id': 1}},
                 {'article_id': 1, 'user_id': 2, 'author': {'id': 2}},
                 {'article_id': 1, 'user_id': 3, 'author': {'id': 3}},
             ]},
             # articles
             {'id': 2, 'user_id': 1, 'comments': [
                 # comments, author
                 {'article_id': 2, 'user_id': 1, 'author': {'id': 1}},
             ]},
         ]},
         okok.Whatever,
         okok.Whatever,
     ]),
])
def test_joined_select(connection: sa.engine.Connection, model: str, query_object: QueryObjectDict, expected_query_lines: list[str], expected_results: list[dict]):
    """ Typical test: JOINs, SQL and results """
    # Models
    Base = sa.orm.declarative_base()

    # One-to-Many, FK on remote side:
    #   User.articles: User -> Article (Article.user_id)
    #   User.comments: User -> Comment (Comment.user_id)
    #   Article.comments: Article -> Comment (Comment.article_id)
    # Many-to-One, FK on local side:
    #   Comment.article: Comment -> Article (Comment.article_id)
    #   Comment.author: Comment -> User (Comment.user_id)
    #   Article.author: Article -> User (Article.user_id)

    class User(IdManyFieldsMixin, Base):
        __tablename__ = 'u'

        articles = sa.orm.relationship('Article', back_populates='author')
        comments = sa.orm.relationship('Comment', back_populates='author')

    class Article(IdManyFieldsMixin, Base):
        __tablename__ = 'a'

        user_id = sa.Column(sa.ForeignKey(User.id))
        author = sa.orm.relationship(User, back_populates='articles')

        comments = sa.orm.relationship('Comment', back_populates='article')

    class Comment(IdManyFieldsMixin, Base):
        __tablename__ = 'c'

        article_id = sa.Column(sa.ForeignKey(Article.id))
        article = sa.orm.relationship(Article, back_populates='comments')

        user_id = sa.Column(sa.ForeignKey(User.id))
        author = sa.orm.relationship(User, back_populates='comments')

    # Data
    with created_tables(connection, Base):
        # Insert some rows
        insert(connection, User, [
            id_manyfields('u', 1),
            id_manyfields('u', 2),
            id_manyfields('u', 3),
        ])
        insert(connection, Article, [
            # 2 articles from User(id=1)
            id_manyfields('a', 1, user_id=1),
            id_manyfields('a', 2, user_id=1),
            # 1 article from User(id=2)
            id_manyfields('a', 3, user_id=2),
            # 1 article from User(id=3)
            id_manyfields('a', 4, user_id=3),
            # article with no user
            # this is a potential stumbling block for conditions that fail to filter it out
            id_manyfields('a', 5, user_id=None),
        ])
        insert(connection, Comment, [
            # User(id=1), User(id=2), User(id=3) commented on Article(id=1)
            id_manyfields('c', 1, user_id=1, article_id=1),
            id_manyfields('c', 2, user_id=2, article_id=1),
            id_manyfields('c', 3, user_id=3, article_id=1),
            # User(id=1) commented on Article(id=2)
            id_manyfields('c', 4, user_id=1, article_id=2),
            # User(id=1) commented on Article(id=3)
            id_manyfields('c', 5, user_id=1, article_id=3),
            # comment with no user/article
            # this is a potential stumbling block for conditions that fail to filter it out
            id_manyfields('c', 6, user_id=None, article_id=None),
        ])

        # Test
        Model = {
            'User': User,
            'Article': Article,
            'Comment': Comment,
        }[model]
        typical_test_query_text_and_results(connection, query_object, Model, expected_query_lines, expected_results)
