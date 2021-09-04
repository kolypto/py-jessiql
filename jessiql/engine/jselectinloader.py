from collections import abc

import collections
import itertools
from typing import Union

import sqlalchemy as sa
import sqlalchemy.orm.strategies

from jessiql.sainfo.version import SA_13, SA_14
from jessiql.sautil.adapt import SimpleColumnsAdapter
from jessiql.typing import SAModelOrAlias, SARowDict


# TODO: Based on SqlAlchemy v1.4.23. Update SqlAlchemy, see if any changes can/should be backported.
#   This is a rolling to do. We need to keep updating.


# Inspired by SelectInLoader._load_for_path()
# With some differences;
# * We ignore the `load_with_join` branch because we can ensure all FKs are loaded
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
        # This object contains joining information
        loader = sa.orm.strategies.SelectInLoader(relation_property, ())
        self.query_info = loader._query_info

    __slots__ = (
        'source_model', 'target_model',
        'source_mapper', 'target_mapper',
        'relation_property', 'key',
        'query_info',
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
        # Use `query_info` from SelectInLoader
        query_info = self.query_info

        # This value is set to `True` when either `relationship(omit_join=False)` was set, or when the left mapper entities
        # do not have FK keys loaded. We do not want these complications since we control how things are loaded.
        # SelectInLoader makes a larger JOIN query in such a case. We don't want that.
        # [ADDED]
        assert not query_info.load_with_join

        # Okay, the `load_with_join` case is excluded.
        # The next thing that controls how a query should be built is `query_info.load_only_child`:
        # it's set to `True` for MANYTOONE relationships, and is `False` for other relationships: ONETOMANY and MANYTOMANY
        # [ADDED]
        assert isinstance(query_info.load_only_child, bool)

        # This is the case of MANYTOONE.
        # This means that we have a list of entities where a foreign key is present within the result set.
        # We need to collect these foreign key columns.
        # Example:
        #   Article.users:
        #       we have a list of `Article[]` where `Article.user_id` is loaded
        #       we'll need to load `User[]` where `User.id = Article.user_id`
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
        # This means that we only have our primary key here, and the foreign key in in that other table.
        # We need to collect our primary keys.
        # Example:
        #   User.articles:
        #       we have a list of `User[]` where `User.id` is loaded
        #       we'll need to load `Article[]` where `Article.user_id = User.id`
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
        """ Prepare the statement for loading: add columns to select, add filter condition """
        # [ADDED] Adapt pk_cols
        # [o] pk_cols = query_info.pk_cols
        adapter = SimpleColumnsAdapter(self.target_model)
        pk_cols = adapter.replace_many(self.query_info.pk_cols)

        # [o] bundle_ent = orm_util.Bundle("pk", *pk_cols)
        # [o] bundle_sql = bundle_ent.__clause_element__()
        # [o] q = Select._create_raw_select(
        # [o] _raw_columns=[bundle_sql, entity_sql],
        if SA_13:
            for col in pk_cols:
                q.append_column(col)
        else:
            q = q.add_columns(*pk_cols)  # [CUSTOMIZED]

        # [o] q = q.filter(in_expr.in_(sql.bindparam("primary_keys")))
        if SA_13:
            q = q.where(
                adapter.replace(  # [ADDED] adapter
                    self.query_info.in_expr.in_(sa.sql.bindparam("primary_keys", expanding=True))
                )
            )
        else:
            q = q.filter(
                adapter.replace(  # [ADDED] adapter
                    self.query_info.in_expr.in_(sa.sql.bindparam("primary_keys"))
                )
            )

        return q

    def fetch_results_and_populate_states(self, connection: sa.engine.Connection, q: sa.sql.Select) -> abc.Iterator[SARowDict]:
        """ Execute the query, fetch results, populate states """
        if self.query_info.load_only_child:
            yield from self._load_via_child(connection, self.our_states, self.none_states, q)  # type: ignore[arg-type]
        else:
            yield from self._load_via_parent(connection, self.our_states, q)  # type: ignore[arg-type]

    # Chunk size: how many related objects to load at once with one SQL IN(...) query
    CHUNKSIZE = sa.orm.strategies.SelectInLoader._chunksize  # type: ignore[attr-defined]

    # Inspired by SelectInLoader._load_via_parent()
    # [o] def _load_via_parent(...):
    def _load_via_parent(self, connection: sa.engine.Connection, our_states: list[SARowDict], q: sa.sql.Select) -> abc.Iterator[SARowDict]:
        # mypy says it might be `None`. We don't want weird relations.
        assert self.relation_property.uselist is not None

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
                key[0] if self.query_info.zero_idx else key
                for key, state_dict in chunk
            ]

            # [o] data = collections.defaultdict(list)
            data: dict[tuple, list[dict]] = collections.defaultdict(list)
            for k, v in itertools.groupby(  # type: ignore[call-overload]
                    # [o] context.session.execute(
                    # [o] q, params={"primary_keys": primary_keys}
                    # [CUSTOMIZED]
                    connection.execute(q, {"primary_keys": primary_keys}),
                    lambda row: get_foreign_key_tuple(row, self.query_info),  # type: ignore[arg-type]
            ):
                # [o] data[k].extend(vv[1] for vv in v)
                # [CUSTOMIZED] convert MappingResult to an actual, mutable dict() to which we'll add keys
                data[k].extend(map(dict, v))


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

    # Inspired by SelectInLoader._load_via_child()
    # [o] def _load_via_child(self, our_states, none_states, query_info, q, context):
    def _load_via_child(self, connection: sa.engine.Connection, our_states: dict[tuple, list], none_states: list[dict], q: sa.sql.Select) -> abc.Iterator[SARowDict]:
        # mypy says it might be `None`. We don't want weird relations.
        assert self.relation_property.uselist is not None

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
                    key[0] if self.query_info.zero_idx else key
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


def get_foreign_key_tuple(row: SARowDict, query_info: sa.orm.strategies.SelectInLoader.query_info) -> tuple:
    """ Get the foreign key tuple from a row dict

    Args:
        row: the dict to pluck from
        query_info: SqlALchemy SelectInLoader.query_info object that contains the necessary information
    """
    return tuple(row[col.key] for col in query_info.pk_cols)
