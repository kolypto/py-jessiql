from collections import abc

import collections
import itertools

import sqlalchemy as sa
import sqlalchemy.orm
import sqlalchemy.orm.strategies

from jessiql.query_object import QueryObject
from jessiql.sainfo.columns import resolve_selected_field
from jessiql.sainfo.relations import resolve_selected_relation
from jessiql.testing.recreate_tables import created_tables


def test_test(connection: sa.engine.Connection):
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
        q_user = QueryObject.from_query_object(dict(
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
                    }
                ),
            },
        ))


        # === Query User
        Model = cls_User = sa.orm.aliased(User)

        stmt = sa.select([]).select_from(Model)

        # Adapter: adapts columns to use the alias
        stmt = stmt.add_columns(*(
            resolve_selected_field(Model, field, where='select')
            for field in q_user.select.fields.values()
        ))
        # Add columns that the relationship wants.
        # Note: duplicate columns will be removed automatically by the select() method
        stmt = stmt.add_columns(
            *select_local_columns_for_relations(Model, q_user, where='select')
        )

        # Get the result, convert list[RowMapping] into list[dict]
        res: sa.engine.CursorResult = connection.execute(stmt)
        loaded_users = [dict(row) for row in res.mappings()]  # TODO: use fetchmany() or partitions()





        # === Query User.articles
        source_Model = cls_User
        target_Model = cls_Article = sa.orm.aliased(Article)

        selected_relation = q_user.select.relations['articles']
        query = selected_relation.query

        select_fields = [
            resolve_selected_field(target_Model, field, where='select')
            for field in query.select.fields.values()
        ]
        # Add columns that relationships wants.
        # Note: duplicate columns will be removed automatically by the select() method
        select_fields.extend(
            select_local_columns_for_relations(source_Model, query, where='select')
        )

        relation_attribute = resolve_selected_relation(source_Model, selected_relation, where='select')
        relation_property: sa.orm.RelationshipProperty = relation_attribute.property

        loaded_articles = list(
            selectinload_relation(connection, source_Model, relation_property, target_Model, loaded_users, select_fields)
        )




        # === Query Article.comments
        source_Model = cls_Article
        target_Model = cls_Comment = sa.orm.aliased(Comment)

        selected_relation = selected_relation.query.select.relations['comments']
        query = selected_relation.query

        select_fields = [
            resolve_selected_field(target_Model, field, where='select')
            for field in query.select.fields.values()
        ]
        # Add columns that relationships wants.
        # Note: duplicate columns will be removed automatically by the select() method
        select_fields.extend(
            select_local_columns_for_relations(source_Model, query, where='select')
        )

        relation_attribute = resolve_selected_relation(source_Model, selected_relation, where='select')
        relation_property: sa.orm.RelationshipProperty = relation_attribute.property

        loaded_comments = list(
            selectinload_relation(connection, source_Model, relation_property, target_Model, loaded_articles, select_fields)
        )




        # === Query Comment.author
        source_Model = cls_Comment
        target_Model = cls_User = sa.orm.aliased(User)

        selected_relation = selected_relation.query.select.relations['author']
        query = selected_relation.query

        select_fields = [
            resolve_selected_field(target_Model, field, where='select')
            for field in query.select.fields.values()
        ]
        # Add columns that relationships wants.
        # Note: duplicate columns will be removed automatically by the select() method
        select_fields.extend(
            select_local_columns_for_relations(source_Model, query, where='select')
        )

        relation_attribute = resolve_selected_relation(source_Model, selected_relation, where='select')
        relation_property: sa.orm.RelationshipProperty = relation_attribute.property

        loaded_authors = list(
            selectinload_relation(connection, source_Model, relation_property, target_Model, loaded_comments, select_fields)
        )



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


class SimpleColumnsAdapter:
    def __init__(self, Model: type):
        adapter = sa.orm.util.ORMAdapter(Model)
        self._replace = adapter.replace

    def replace(self, obj):
        return sa.sql.visitors.replacement_traverse(obj, {}, self._replace)

    def replace_many(self, objs: abc.Iterable) -> abc.Iterator:
        yield from (
            self.replace(obj)
            for obj in objs
        )


class LeftRelationshipColumnsAdapter(SimpleColumnsAdapter):
    def __init__(self, left_model: type, relation_property: sa.orm.RelationshipProperty):
        right_mapper = relation_property.mapper
        adapter = sa.orm.util.ORMAdapter(left_model, equivalents=right_mapper._equivalent_columns if right_mapper else {})
        self._replace = adapter.replace


# Inspired by SelectInLoader._load_for_path() , SqlAlchemy v1.4.15
# With some differences;
# * We ignore the `load_with_join` branch because we can ensure all FKs are loaded
# * We do no streaming: all parent FKs are available at once, so we don't care about `order_by`s
# * We don't use baked queries for now
# * Some customizations are marked with [CUSTOMIZED]
# * Some additions are marked with [ADDED]
def selectinload_relation(connection, source_model: type, relation_property: sa.orm.RelationshipProperty, target_model: type, states: list[dict], select_fields: list[sa.sql.ClauseElement]) -> abc.Iterator[dict]:
    print(f'=== {source_model} {relation_property} {target_model}', list(map(str, select_fields)))

    source_mapper: sa.orm.Mapper = source_model.__mapper__
    target_mapper: sa.orm.Mapper = relation_property.mapper

    # Use the SelectInLoader to generate parts of the query for us
    loader = sa.orm.strategies.SelectInLoader(relation_property, ())
    query_info = loader._query_info

    # This value is set to `True` when either `relationship(omit_join=False)` was set, or when the left mapper entities
    # do not have FK keys loaded. We do not want these complications since we control how things are loaded.
    # SelectInLoader makes a larger JOIN query in such a case. We don't want that.
    assert not query_info.load_with_join

    # Okay, the `load_with_join` case is excluded.
    # The next thing that controls how a query should be built is `query_info.load_only_child`:
    # it's set to `True` for MANYTOONE relationships, and is `False` for other relationships: ONETOMANY and MANYTOMANY
    assert isinstance(query_info.load_only_child, bool)

    # This is the case of MANYTOONE.
    # This means that we have a list of entities where a foreign key is present within the result set.
    # We need to collect these foreign key columns.
    # Example:
    #   Article.users:
    #       we have a list of `Article[]` where `Article.user_id` is loaded
    #       we'll need to load `User[]` where `User.id = Article.user_id`
    if query_info.load_only_child:
        our_states = collections.defaultdict(list)
        none_states = []

        for state_dict in states:
            related_ident = tuple(
                state_dict[lk.key]
                for lk in query_info.child_lookup_cols
            )

            # organize states into lists keyed to particular foreign key values.
            if None not in related_ident:
                our_states[related_ident].append(state_dict)
            else:
                # For FK values that have None, add them to a separate collection that will be populated separately
                none_states.append(state_dict)

    # This is the case of ONETOMANY and MANYTOMANY.
    # This means that we only have our primary key here, and the foreign key in in that other table.
    # We need to collect our primary keys.
    # Example:
    #   User.articles:
    #       we have a list of `User[]` where `User.id` is loaded
    #       we'll need to load `Article[]` where `Article.user_id = User.id`
    if not query_info.load_only_child:
        # If it fails to find a column in `state`, it means the `state` does not have a primary key loaded
        our_states = [
            (get_primary_key_tuple(source_mapper, state), state)
            for state in states
        ]

    # [ADDED] Adapt pk_cols
    adapter = SimpleColumnsAdapter(target_model)
    pk_cols = adapter.replace_many(query_info.pk_cols)
    pk_cols = list(pk_cols)

    q = sa.select(pk_cols)

    # [ADDED] Load other fields that we want
    q = q.add_columns(*select_fields)

    if not query_info.load_with_join:
        q = q.select_from(target_model)

    q = q.filter(
        adapter.replace(  # [ADDED] adapter
            query_info.in_expr.in_(sa.sql.bindparam("primary_keys"))
        )
    )

    if query_info.load_only_child:
        yield from _load_via_child(connection, relation_property, our_states, none_states, query_info, q)
    else:
        yield from _load_via_parent(connection, relation_property, our_states, query_info, q)


def select_local_columns_for_relations(Model: type, q: QueryObject, *, where: str):
    for relation in q.select.relations.values():
        relation_attribute = resolve_selected_relation(Model, relation, where=where)
        relation_property: sa.orm.RelationshipProperty = relation_attribute.property

        adapter = LeftRelationshipColumnsAdapter(Model, relation_property)
        yield from adapter.replace_many(relation_property.local_columns)


def get_primary_key_tuple(mapper: sa.orm.Mapper, row: dict) -> tuple:
    """ Get the primary key tuple from a row dict

    Args:
        mapper: the Mapper to get the primary key from
        row: the dict to
    """
    return tuple(row[col.key] for col in mapper.primary_key)


def get_foreign_key_tuple(row: dict, query_info: sa.orm.strategies.SelectInLoader.query_info) -> tuple:
    return tuple(row[col.key] for col in query_info.pk_cols)


CHUNKSIZE = sa.orm.strategies.SelectInLoader._chunksize


# Inspired by SelectInLoader._load_via_parent() , SqlAlchemy v1.4.15
def _load_via_parent(connection: sa.engine.Connection, relation_property: sa.orm.RelationshipProperty, our_states: list[dict], query_info: sa.orm.strategies.SelectInLoader.query_info, q: sa.sql.Select) -> abc.Iterator[dict]:
    relation_key: str = relation_property.key
    uselist: bool = relation_property.uselist
    _empty_result = () if uselist else None

    while our_states:
        chunk = our_states[0: CHUNKSIZE]
        our_states = our_states[CHUNKSIZE:]

        primary_keys = [
            key[0] if query_info.zero_idx else key
            for key, state_dict in chunk
        ]

        data = collections.defaultdict(list)
        for k, v in itertools.groupby(
                # [CUSTOMIZED]
                connection.execute(q, {"primary_keys": primary_keys}).mappings(),#.unique()
                lambda row: get_foreign_key_tuple(row, query_info),
        ):
            data[k].extend(
                map(dict, v)  # [CUSTOMIZED] convert MappingResult to an actual, mutable dict() to which we'll add keys
            )

        for key, state_dict in chunk:
            collection = data.get(key, _empty_result)

            if not uselist and collection:
                if len(collection) > 1:
                    sa.util.warn(f"Multiple rows returned with uselist=False for attribute {relation_property}")
                state_dict[relation_key] = collection[0]  # [CUSTOMIZED]
            else:
                state_dict[relation_key] = collection  # [CUSTOMIZED]

            # [ADDED] Return loaded objects
            yield from collection


# Inspired by SelectInLoader._load_via_child() , SqlAlchemy v1.4.15
def _load_via_child(connection: sa.engine.Connection, relation_property: sa.orm.RelationshipProperty, our_states: dict[tuple, list], none_states: list[dict], query_info: sa.orm.strategies.SelectInLoader.query_info, q: sa.sql.Select) -> abc.Iterator[dict]:
    relation_key: str = relation_property.key
    mapper: sa.orm.Mapper = relation_property.mapper
    uselist: bool = relation_property.uselist

    # this sort is really for the benefit of the unit tests
    our_keys = sorted(our_states)
    while our_keys:
        chunk = our_keys[0: CHUNKSIZE]
        our_keys = our_keys[CHUNKSIZE:]

        data = {
            get_primary_key_tuple(mapper, row): dict(row)  # [CUSTOMIZED] Convert mappings into mutable dicts
            for row in connection.execute(q, {"primary_keys": [
                key[0] if query_info.zero_idx else key
                for key in chunk
            ]}).mappings()
        }

        for key in chunk:
            related_obj = data.get(key, None)
            for state_dict in our_states[key]:
                state_dict[relation_key] = related_obj if not uselist else [related_obj]

        # populate none states with empty value / collection
        for state_dict in none_states:
            state_dict[relation_key] = None

        # [ADDED] Return loaded objects
        yield from data.values()
