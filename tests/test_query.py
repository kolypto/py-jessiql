import pytest
import sqlalchemy as sa
import sqlalchemy.orm

from jessiql import QueryObjectDict, Query
from jessiql.util import sacompat
from .util.models import IdManyFieldsMixin
from .util.test_queries import assert_query_statements_lines


@pytest.mark.parametrize(('query_object', 'expected_columns',), [
    # Primary model: filtered
    (dict(), ['WHERE a.user_id = 1']),
    # Joined model: filtered
    (dict(select=[
        {'author': dict()},
        {'comments': dict()},
    ]), [
        # Make sure it got executed on every level
        'WHERE a.user_id = 1',
        'AND u.id = 1',
        'AND c.user_id = 1',
    ]),
    # Joined model: filtered
])
def test_query_customize_statements(connection: sa.engine.Connection, query_object: QueryObjectDict, expected_columns: list[str]):
    """ Test Query.customize_statements: adding security to queries """
    # Models
    Base = sacompat.declarative_base()

    class User(IdManyFieldsMixin, Base):
        __tablename__ = 'u'

    class Article(IdManyFieldsMixin, Base):
        __tablename__ = 'a'

        user_id = sa.Column(sa.ForeignKey(User.id))

        author = sa.orm.relationship(User)
        comments = sa.orm.relationship('Comment')

    class Comment(IdManyFieldsMixin, Base):
        __tablename__ = 'c'

        article_id = sa.Column(sa.ForeignKey(Article.id))
        user_id = sa.Column(sa.ForeignKey(User.id))

    # Query
    q = Query(query_object, Article)

    # Security
    @q.customize_statements.append
    def security(q: Query, stmt: sa.sql.Select) -> sa.sql.Select:
        """ Security: make sure that the user can only access their own data """
        ALLOWED_USER_ID = 1

        path = q.load_path
        if path == (Article,):
            return stmt.where(q.Model.user_id == ALLOWED_USER_ID)
        elif path == (Article, 'author', User):
            return stmt.where(q.Model.id == ALLOWED_USER_ID)
        elif path == (Article, 'comments', Comment):
            return stmt.where(q.Model.user_id == ALLOWED_USER_ID)
        else:
            raise NotImplementedError

    # SQL
    assert_query_statements_lines(q, *expected_columns)


def test_query_filter(connection: sa.engine.Connection):
    """ Test Query.filter() """
    def main():
        query_object = dict()
        q = Query(query_object, User)
        q.filter(User.id == 1)

        assert_query_statements_lines(q, 'WHERE u.id = 1')

    # Models
    Base = sacompat.declarative_base()

    class User(IdManyFieldsMixin, Base):
        __tablename__ = 'u'
    
    # Go
    main()