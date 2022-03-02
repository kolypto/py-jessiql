""" Query Object tools for an SqlAlchemy model """

from functools import lru_cache

import graphql

import sqlalchemy as sa
import sqlalchemy.orm

from jessiql import sainfo
from .defs import FieldQueryInfo, QUERY_FIELD_SKIP


class QueryModelField:
    """ Query Field func for an SqlAlchemy model

    Example use:
        def resolve_user(obj, info):
            query_object = query_object_for(info, runtime_type='Model', field_query=QueryModelField(Model))
    """

    def __init__(self, Model: type):
        """ Initialize the Query Field func with an SqlAlchemy model

        This model will become the starting point.
        All sub-field requests will be resolved against its attributes.

        Example:
            query = '''
                query {
                    user {
                        login
                        articles { title }
                    }
                }
            '''
            QueryModelField(User):
            * will resolve 'login' against `User`
            * will resolve 'articles' against `User`, assuming it's a relationship to `Article`
            * will resolve 'title' against `Article`

        Args:
            Model: The model to resolve fields against.
        """
        self.Model = Model
        self.mapper: sa.orm.Mapper = sa.orm.class_mapper(Model)

    def __call__(self, field_name: str, field_def: graphql.type.definition.GraphQLField, path: tuple[str, ...]) -> FieldQueryInfo:
        # Follow path, get nested mapper
        mapper = get_mapper_for_path(self.mapper, path)  # raises: KeyError for unknown attributes in `path`

        # Property?
        if sainfo.properties.is_property(mapper.class_, field_name):
            return FieldQueryInfo(select=[field_name])

        # Get field info, skip it if not known to SqlAlchemy
        try:
            attr = mapper.all_orm_descriptors[field_name]
        except KeyError:
            return QUERY_FIELD_SKIP

        # Check whether `field_name` is a column or a relationship.
        # It is important that we reuse the same `sainfo` logic that JessiQL uses!
        # In this case, we'll know which fields are supported and which are not.

        # Column?
        if sainfo.columns.is_column(attr):
            return FieldQueryInfo(select=[field_name])
        # Relationship?
        elif sainfo.relations.is_relation(attr):
            return FieldQueryInfo(join_name=field_name)
        # Some unsupported attribute
        else:
            return QUERY_FIELD_SKIP


@lru_cache(maxsize=100)
def get_mapper_for_path(mapper: sa.orm.Mapper, path: tuple[str, ...]) -> sa.orm.Mapper:
    """ Follow `path` of attribute names, get down to the mapper through relationships

    Args:
        mapper: the mapper to start with
        path: A list of relationship names to follow

    Returns:
        Mapper: the final mapper

    Raises:
        KeyError: some attribute name is unknown
    """
    # Follow path
    for attr_name in path:
        relationship = mapper.relationships[attr_name]  # raises: KeyError for unknown attributes in `path`
        mapper = relationship.mapper  # target mapper

    # Done
    return mapper
