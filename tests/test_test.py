from collections import abc

import sqlalchemy as sa

from jessiql import operations
from jessiql.query_object import QueryObject
from jessiql.query_object import SelectedRelation
from jessiql.sautil.jselectinloader import JSelectInLoader
from jessiql.testing.recreate_tables import created_tables
from jessiql.typing import SAModelOrAlias, SARowDict


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



        from sqlalchemy.orm import Session
        from contextlib import closing

        with closing(Session(bind=connection)) as ssn:
            print('=== User.articles')
            users = ssn.query(User).options(sa.orm.selectinload(User.articles)).all()

        with closing(Session(bind=connection)) as ssn:
            print('=== Article.author')
            articles = ssn.query(Article).options(sa.orm.selectinload(Article.author)).all()

        print('\n'*5)



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
                                )
                            }
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

        def simple_query(connection: sa.engine.Connection, target_Model: SAModelOrAlias, query: QueryObject) -> abc.Iterator[SARowDict]:
            stmt = sa.select([]).select_from(target_Model)

            select_op = operations.SelectOperation(query, target_Model)
            stmt = select_op.apply_to_statement(stmt)
            filter_op = operations.FilterOperation(query, target_Model)
            stmt = filter_op.apply_to_statement(stmt)
            sort_op = operations.SortOperation(query, target_Model)
            stmt = sort_op.apply_to_statement(stmt)
            skiplimit_op = operations.SkipLimitOperation(query, target_Model)
            stmt = skiplimit_op.apply_to_statement(stmt)

            print(str(stmt))

            # Get the result, convert list[RowMapping] into list[dict]
            res: sa.engine.CursorResult = connection.execute(stmt)
            yield from (dict(row) for row in res.mappings())  # TODO: use fetchmany() or partitions()

        def joined_query(connection: sa.engine.Connection, source_Model: SAModelOrAlias, target_Model: SAModelOrAlias, selected_relation: SelectedRelation, source_states: list[SARowDict]):
            query = selected_relation.query

            stmt = sa.select([]).select_from(target_Model)

            # Joined Loader
            loader = JSelectInLoader(source_Model, selected_relation.property, target_Model)
            loader.prepare_states(source_states)
            stmt = loader.prepare_query(stmt)

            select_op = operations.SelectOperation(query, target_Model)
            stmt = select_op.apply_to_statement(stmt)
            filter_op = operations.FilterOperation(query, target_Model)
            stmt = filter_op.apply_to_statement(stmt)
            sort_op = operations.SortOperation(query, target_Model)
            stmt = sort_op.apply_to_statement(stmt)

            # NOTE: this has to be done last, because it wraps everything into a subquery, and a different alias has to be used
            # in order to refer to columns of this query.
            skiplimit_op = operations.SkipLimitOperation(query, target_Model)
            stmt = skiplimit_op.apply_to_related_statement(
                # This one is different from top-level statement because we have to use window functions for pagination
                stmt,
                selected_relation.property.remote_side
            )

            print(str(stmt))

            # Done
            yield from loader.fetch_results_and_populate_states(connection, stmt)


        # === Query User
        loaded_users = list(simple_query(
            connection,
            target_Model=(cls_User := sa.orm.aliased(User)),
            query=query
        ))


        # === Query User.articles
        loaded_articles = list(joined_query(
            connection,
            source_Model=cls_User,
            target_Model=sa.orm.aliased(Article),
            selected_relation=(selected_relation := query.select.relations['articles']),
            source_states=loaded_users,
        ))



        # === Query Article.comments
        loaded_comments = list(joined_query(
            connection,
            source_Model=Article,
            target_Model=sa.orm.aliased(Comment),
            selected_relation=(selected_relation := selected_relation.query.select.relations['comments']),
            source_states=loaded_articles,
        ))



        # === Query Comment.author
        loaded_authors = list(joined_query(
            connection,
            source_Model=Comment,
            target_Model=sa.orm.aliased(User),
            selected_relation=(selected_relation := selected_relation.query.select.relations['author']),
            source_states=loaded_comments,
        ))


        __import__('pprint').pprint(loaded_users)


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
