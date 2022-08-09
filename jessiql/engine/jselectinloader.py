from collections import abc

import collections
import itertools
from typing import Union

import sqlalchemy as sa
import sqlalchemy.orm.strategies

from jessiql.sautil.adapt import SimpleColumnsAdapter
from jessiql.typing import SAModelOrAlias, SARowDict
from jessiql.util.sacompat import add_columns, SARow, stmt_filter
from jessiql.sainfo.version import SA_13, SA_14


# TODO: Based on SqlAlchemy v1.4.23. Update SqlAlchemy, see if any changes can/should be backported.
#   This is a rolling to do. We need to keep updating.
# Find diffs:
# https://github.com/sqlalchemy/sqlalchemy/compare/rel_1_4_23...rel_1_4_24
# $ git diff rel_1_4_23..rel_1_4_24 -- lib/sqlalchemy/orm/strategies.py

# Inspired by SelectInLoader._load_for_path()
# With some differences;
# * We do no streaming: all parent FKs are available at once, so we don't care about `order_by`s
# * We don't use baked queries for now
# * Some customizations are marked with [CUSTOMIZED]
# * Some additions are marked with [ADDED]
# * Anchor lines in the original SqlAlchemy code marked [o] with original code samples provided


class JSelectInLoader:
    """ Loader for related objects, borrowing the approach from SqlAlchemy's SelectInLoader

    Overall principle:
    * `source_model` objects have already been loaded. They're called "states"
    * `relation_property` is the relationship we're going to load. It points to `target_model`.
    * `prepare_states()` collects states (loaded objects) and foreign keys to be used for loading
    * `prepare_query()` adds required fields to the SELECT statements
    * `fetch_results_and_populate_states()` populates existing objects ("states") with loaded relation fields
    """
    def __init__(self, source_model: SAModelOrAlias, relation_property: sa.orm.RelationshipProperty, target_model: SAModelOrAlias):
        """

        Args:
            source_model: The parent model. Its objects have already been loaded.
            relation_property: The joined relationship: the one that should be loaded
            target_model: The model that the relationship points to.
        """
        # Models (or aliases)
        self.source_model = source_model
        self.target_model = target_model

        # Mappers
        self.source_mapper: sa.orm.Mapper = sa.orm.class_mapper(source_model)
        self.target_mapper: sa.orm.Mapper = sa.orm.class_mapper(target_model)

        # Relationship and its name (key)
        self.relation_property = relation_property
        self.key = relation_property.key

        # Use `SelectInLoader` to produce `query_info` for us. We'll reuse it.
        self.loader = sa.orm.strategies.SelectInLoader(relation_property, ())

        # Prefix for columns that we add to the query
        # Typically: "tablename.". Yes, with a period.
        self.fk_label_prefix = ''

    __slots__ = (
        'source_model', 'target_model',
        'source_mapper', 'target_mapper',
        'relation_property', 'key',
        'loader',
        'fk_label_prefix',
        'our_states', 'none_states',
    )

    # When `query_info.load_only_child`, it's a `dict`
    # Otherwise, it's a `list`
    our_states: Union[dict, list]

    # Inspired by SelectInLoader._load_for_path(), part 1
    # [o] def _load_for_path(...)
    def prepare_states(self, states: list[SARowDict]):
        """ Get the states to be populated, gather information from them

        Note that states haven't been provided to the constructor.
        This is the first time this class sees them.
        """
        # Use SelectInLoader
        query_info = self.loader._query_info

        # SelectInLoader will handle two cases:
        # * If the select query contains the foreign key we need.
        #   Example with User.articles:
        #       SELECT id, author_id FROM articles
        #   Then it will use `init_for_omit_join()`: this means that it does not have to add anything to the query.
        # * If we cannot select everything we need from just one table (which is the case for M2M relationships),
        #   SelectInLoader will use `init_for_join()` and produce a SELECT ... FROM ... JOIN.

        # Controls how a query should be built is `query_info.load_only_child`:
        # it's set to `True` for MANYTOONE relationships,
        # and is `False` for other relationships: ONETOMANY and MANYTOMANY
        # [ADDED]
        assert isinstance(query_info.load_only_child, bool)

        # This is the case of MANYTOONE.
        # We are handling a child relationship which has a parent -- mentioned via an FK.
        # That is, we have a foreign key that refers to a parent.
        # Example:
        #   Article.users:
        #       we have a list of `Article[]` where `Article.user_id` is present within the result set.
        #       we'll need to load `User[]` where `User.id = Article.user_id`
        # We need to collect these foreign key columns.
        # [o] if query_info.load_only_child:
        if query_info.load_only_child:
            # [o] our_states = collections.defaultdict(list)
            self.our_states = collections.defaultdict(list)
            self.none_states = []

            # [o] for state, overwrite in states:
            for state_dict in states:
                # [o] related_ident = tuple(...)
                related_ident = tuple(
                    state_dict[lk.key]
                    for lk in query_info.child_lookup_cols
                )

                # organize states into lists keyed to particular foreign key values.
                # [o] if None not in related_ident:
                if None not in related_ident:
                    self.our_states[related_ident].append(state_dict)
                else:
                    # For FK values that have None, add them to a separate collection that will be populated separately
                    self.none_states.append(state_dict)

        # This is the case of ONETOMANY and MANYTOMANY.
        # We are handling an object that is referenced by a parent somewhere out there.
        # That is, our primary key is mentioned by the parent entity.
        # Example:
        #   User.articles:
        #       we have a list of `User[]` where `User.id` is loaded
        #       we'll need to load `Article[]` where `Article.user_id = User.id`
        # We need to collect our primary keys.
        # [o] if not query_info.load_only_child:
        if not query_info.load_only_child:
            # If it fails to find a column in `state`, it means the `state` does not have a primary key loaded
            self.our_states = [
                (get_primary_key_tuple(self.source_mapper, state), state)
                for state in states
            ]

    # Inspired by SelectInLoader._load_for_path(), part 2
    # [o] def _load_for_path(...)
    def prepare_query(self, q: sa.sql.Select) -> sa.sql.Select:
        """ Prepare the statement for loading: add columns to select, add filter condition

        Args:
            q: SELECT statement prepared by QueryExecutor.statement().
               It has no columns yet, but has a select_from(self.target_model), unaliased.
               NOTE: we never alias the target model: the one we're loading. It would've made things too complicated.
        """
        # Use SelectInLoader
        # self.query_info: primary key columns, the IN expression, etc
        # self._parent_alias: used with JOINed relationships where our table has to be joined to an alias of the parent table
        query_info = self.loader._query_info
        parent_alias = self.loader._parent_alias if query_info.load_with_join else NotImplemented
        effective_entity = self.target_model

        # [ADDED] Adapt pk_cols
        # [o] pk_cols = query_info.pk_cols
        # [o] in_expr = query_info.in_expr
        pk_cols = query_info.pk_cols
        in_expr = query_info.in_expr

        # [o] if not query_info.load_with_join:
        if not query_info.load_with_join:
            # [o] if effective_entity.is_aliased_class:
            # [o]     pk_cols = [ effective_entity._adapt_element(col) for col in pk_cols ]
            # [o]     in_expr = effective_entity._adapt_element(in_expr)
            adapter = SimpleColumnsAdapter(self.target_model)
            pk_cols = adapter.replace_many(pk_cols)
            in_expr = adapter.replace(in_expr)

        # [o] bundle_ent = orm_util.Bundle("pk", *pk_cols)
        # [o] entity_sql = effective_entity.__clause_element__()
        # [o] q = Select._create_raw_select(
        # [o]     _raw_columns=[bundle_sql, entity_sql],
        # [o]     _label_style=LABEL_STYLE_TABLENAME_PLUS_COL,
        # [CUSTOMIZED]
        if not query_info.load_with_join:
            q = add_columns(q, pk_cols)  # [CUSTOMIZED]
        else:
            # NOTE: we cannot always add our FK columns: when `load_with_join` is used, these columns
            # may actually refer to columns from a M2M table with conflicting names!
            # Example:
            #   SELECT articles.id, tags.id
            #   FROM articles JOIN ... JOIN tags
            # So we have to rename them. We use "table.column" aliases because this horrible "." makes it clear
            # it's not just another column
            # label_prefix = self.source_model.__table__.name + '.'
            self.fk_label_prefix = self.source_model.__tablename__ + '.'  # type: ignore[union-attr]
            q = add_columns(q, [  # [CUSTOMIZED]
                col.label(self.fk_label_prefix + col.key)
                for col in pk_cols
            ])

        # Effective entity
        # This is the class that we select from
        # [o] if not query_info.load_with_join:
        # [o]     q = q.select_from(effective_entity)
        # [o] else:
        # [o]     q = q.select_from(self._parent_alias).join(...)
        # [CUSTOMIZED]
        if not query_info.load_with_join:
            q = q.select_from(self.target_model)
        else:
            if SA_13:
                q = q.select_from(
                    sa.orm.join(parent_alias, self.target_model, onclause=getattr(parent_alias, self.key).of_type(self.target_model))
                )
            elif SA_14:
                q = q.select_from(parent_alias).join(
                    getattr(parent_alias, self.key).of_type(self.target_model)
                )
            else:
                raise NotImplementedError

        # [o] q = q.filter(in_expr.in_(sql.bindparam("primary_keys")))
        q = stmt_filter(q, in_expr.in_(sa.sql.bindparam("primary_keys", expanding=True)))

        return q

    def fetch_results_and_populate_states(self, connection: sa.engine.Connection, q: sa.sql.Select) -> abc.Iterator[SARowDict]:
        """ Execute the query, fetch results, populate states """
        query_info = self.loader._query_info

        if query_info.load_only_child:
            yield from self._load_via_child(connection, self.our_states, self.none_states, q)  # type: ignore[arg-type]
        else:
            yield from self._load_via_parent(connection, self.our_states, q)  # type: ignore[arg-type]

    # Chunk size: how many related objects to load at once with one SQL IN(...) query
    CHUNKSIZE = sa.orm.strategies.SelectInLoader._chunksize  # type: ignore[attr-defined]

    # Used for: ONETOMANY and MANYTOMANY. That is, our primary key is mentioned by the parent entity.
    # Inspired by SelectInLoader._load_via_parent()
    # [o] def _load_via_parent(...):
    def _load_via_parent(self, connection: sa.engine.Connection, our_states: list[SARowDict], q: sa.sql.Select) -> abc.Iterator[SARowDict]:
        # mypy says it might be `None`. We don't want undefined behavior. Configure your relationships first.
        assert self.relation_property.uselist is not None

        # Use SelectInLoader
        query_info = self.loader._query_info
        label_prefix = f'' if query_info.load_with_join else ''

        # [o] uselist = self.uselist
        uselist: bool = self.relation_property.uselist
        # [o] _empty_result = () if uselist else None
        _empty_result: abc.Callable[[], Union[list, None]] = lambda: [] if uselist else None

        # [o]
        while our_states:
            # [o]
            chunk = our_states[0: self.CHUNKSIZE]
            our_states = our_states[self.CHUNKSIZE:]

            # [o]
            primary_keys = [
                key[0] if query_info.zero_idx else key
                for key, state_dict in chunk
            ]

            # [o] data = collections.defaultdict(list)
            data: dict[tuple, list[dict]] = collections.defaultdict(list)
            for k, v in itertools.groupby(  # type: ignore[call-overload]
                    # [o] context.session.execute(
                    # [o] q, params={"primary_keys": primary_keys}
                    # [CUSTOMIZED]
                    connection.execute(q, {"primary_keys": primary_keys}),
                    lambda row: get_foreign_key_tuple(row, query_info.pk_cols, self.fk_label_prefix),  # type: ignore[arg-type]
            ):
                # [o] data[k].extend(vv[1] for vv in v)
                # [CUSTOMIZED] convert MappingResult to an actual, mutable dict() to which we'll add keys
                data[k].extend(row_without_fk_columns(row, self.fk_label_prefix) for row in v)


            # [o] for key, state, state_dict, overwrite in chunk:
            for key, state_dict in chunk:
                # [o]
                collection = data.get(key, _empty_result())

                # [o]
                if not uselist and collection:
                    if len(collection) > 1:
                        sa.util.warn(f"Multiple rows returned with uselist=False for attribute {self.relation_property}")

                    # [o] state.get_impl(self.key).set_committed_value(state, state_dict, collection[0])
                    state_dict[self.key] = collection[0]  # [CUSTOMIZED]
                else:
                    # [o] state.get_impl(self.key).set_committed_value(state, state_dict, collection)
                    state_dict[self.key] = collection  # [CUSTOMIZED]

                # [ADDED] Return loaded objects
                if uselist:
                    yield from collection  # type: ignore[misc]
                else:
                    yield collection  # type: ignore[misc]

    # Used for: MANYTOONE. That is, we have a foreign key that refers to a parent.
    # Inspired by SelectInLoader._load_via_child()
    # [o] def _load_via_child(self, our_states, none_states, query_info, q, context):
    def _load_via_child(self, connection: sa.engine.Connection, our_states: dict[tuple, list], none_states: list[dict], q: sa.sql.Select) -> abc.Iterator[SARowDict]:
        # mypy says it might be `None`. We don't want undefined behavior. Configure your relationships first.
        assert self.relation_property.uselist is not None

        # Use SelectInLoader
        query_info = self.loader._query_info

        # we do not support relationships with JOIN here (because they have aliased column names)
        assert not query_info.load_with_join

        # [o] uselist = self.uselist
        uselist: bool = self.relation_property.uselist

        # this sort is really for the benefit of the unit tests
        # [o] our_keys = sorted(our_states)
        our_keys = sorted(our_states)
        # [o]
        while our_keys:
            # [o] chunk = our_keys[0 : self._chunksize]
            # [o] our_keys = our_keys[self._chunksize :]
            chunk = our_keys[0: self.CHUNKSIZE]
            our_keys = our_keys[self.CHUNKSIZE:]

            # [o]
            data = {
                get_primary_key_tuple(self.target_mapper, row): dict(row)  # type: ignore[arg-type]   # [CUSTOMIZED] Convert mappings into mutable dicts
                # [o] for k, v in context.session.execute(
                for row in connection.execute(q, {"primary_keys": [
                    key[0] if query_info.zero_idx else key
                    for key in chunk
                ]})
            }

            # [o]
            for key in chunk:
                # [o]
                related_obj = data.get(key, None)

                # [o] for state, dict_, overwrite in our_states[key]:
                for state_dict in our_states[key]:
                    # [o] state.get_impl(self.key).set_committed_value(
                    # [o]     related_obj if not uselist else [related_obj],
                    state_dict[self.key] = related_obj if not uselist else [related_obj]

            # populate none states with empty value / collection
            # [o] for state, dict_, overwrite in none_states:
            for state_dict in none_states:
                # [o] state.get_impl(self.key).set_committed_value(state, dict_, None)
                state_dict[self.key] = None

            # [ADDED] Return loaded objects
            yield from data.values()


def get_primary_key_tuple(mapper: sa.orm.Mapper, row: SARowDict) -> tuple:
    """ Get the primary key tuple from a row dict

    Args:
        mapper: the Mapper to get the primary key from
        row: the dict to pluck from
    """
    return tuple(row[col.key] for col in mapper.primary_key)


def get_foreign_key_tuple(row: SARowDict, pk_cols: abc.Iterable[sa.Column], fk_label_prefix: str) -> tuple:
    """ Get the foreign key tuple from a row dict

    Args:
        row: the dict to pluck from
        query_info: SqlALchemy SelectInLoader.query_info object that contains the necessary information
    """
    return tuple(
        row[fk_label_prefix + col.key]  # type: ignore[operator]
        for col in pk_cols
    )

def row_without_fk_columns(row: SARow, fk_label_prefix: str) -> dict:
    """ Get the row, drop qualified columns

    JSelectInLoader adds some service columns: these have a special name with a period: "table.column".
    We will remove such columns using prefix test
    """
    # No prefix? Nothing to strip. Convert the row to dict.
    if fk_label_prefix == '':
        return dict(row)
    # Prefix? We need to drop some columns
    else:
        return {
            k: v
            for k, v in dict(row).items()
            if not k.startswith(fk_label_prefix)
        }