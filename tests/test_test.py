import sqlalchemy as sa

from jessiql.query import QueryExecutor
from jessiql.query_object import QueryObject
from jessiql.testing.recreate_tables import created_tables


def test_joins_many_levels(connection: sa.engine.Connection):
    with created_tables(connection, Base):
        # Insert some values
        stmt = sa.insert(User).values([
            id_manyfields('user', 1),
            id_manyfields('user', 2),
            id_manyfields('user', 3),
        ])
        connection.execute(stmt)

        stmt = sa.insert(Article).values([
            # 2 articles from User(id=1)
            id_manyfields('article', 1, user_id=1),
            id_manyfields('article', 2, user_id=1),
            # 1 article from User(id=2)
            id_manyfields('article', 3, user_id=2),
            # 1 article from User(id=3)
            id_manyfields('article', 4, user_id=3),
        ])
        connection.execute(stmt)

        stmt = sa.insert(Comment).values([
            # User(id=1), User(id=2), User(id=3) commented on Article(id=1)
            id_manyfields('comment', 1, user_id=1, article_id=1),
            id_manyfields('comment', 2, user_id=2, article_id=1),
            id_manyfields('comment', 3, user_id=3, article_id=1),
            # User(id=1) commented on Article(id=2)
            id_manyfields('comment', 4, user_id=1, article_id=2),
            # User(id=1) commented on Article(id=3)
            id_manyfields('comment', 5, user_id=1, article_id=3),
        ])
        connection.execute(stmt)


        # Prepare some sample input object
        query = QueryObject.from_query_object(dict(
            # Top level: the primary entity
            select=['id', 'a'],
            join={
                # Second level: the related entity, one-to-many
                'articles': dict(
                    select=['a'],
                    join={
                        # Third level: the related entity, one-to-many
                        'comments': dict(
                            select=['a'],
                            join={
                                # Fourth level: the related entity, one-to-many (!)
                                'author': dict(
                                    select=['a'],
                                    limit=100,
                                )
                            },
                            limit=100,
                        ),
                    },
                    sort=['id-'],
                    limit=100,
                ),
            },
            sort=['id-'],
            filter={
                '$and': [
                    {'id': {'$gt': 0}},
                    {'id': {'$gte': 0}},
                ]
            },
        ))

        # === Query User
        q = QueryExecutor(query, User)
        users = q.fetchall(connection)

        __import__('pprint').pprint(users)


    __import__('pytest').fail(pytrace=False)


Base = sa.orm.declarative_base()


class ManyFieldsMixin:
    a = sa.Column(sa.String)
    b = sa.Column(sa.String)
    c = sa.Column(sa.String)
    d = sa.Column(sa.String)


def manyfields(prefix: str, n: int):
    return {
        k: f'{prefix}-{n}-a'
        for k in 'abcd'
    }


def id_manyfields(prefix: str, id: int, **extra):
    return {
        'id': id,
        **manyfields(prefix, id),
        **extra
    }


class User(ManyFieldsMixin, Base):
    __tablename__ = 'users'

    id = sa.Column(sa.Integer, primary_key=True)
    articles = sa.orm.relationship('Article', back_populates='author')
    comments = sa.orm.relationship('Comment', back_populates='author')


class Article(ManyFieldsMixin, Base):
    __tablename__ = 'articles'

    id = sa.Column(sa.Integer, primary_key=True)

    user_id = sa.Column(sa.ForeignKey(User.id))
    author = sa.orm.relationship(User, back_populates='articles')

    comments = sa.orm.relationship('Comment', back_populates='article')


class Comment(ManyFieldsMixin, Base):
    __tablename__ = 'comments'
    id = sa.Column(sa.Integer, primary_key=True)

    article_id = sa.Column(sa.ForeignKey(Article.id))
    article = sa.orm.relationship(Article, back_populates='comments')

    user_id = sa.Column(sa.ForeignKey(User.id))
    author = sa.orm.relationship(User, back_populates='comments')
