from __future__ import annotations

import dataclasses
from collections import abc
from typing import Optional, Union
import sqlalchemy as sa
import sqlalchemy.orm

from jessiql import sainfo

from .base import FieldRenamer, FieldContext, UnknownFieldError

@dataclasses.dataclass
class FieldsMap(FieldRenamer):
    """ Fields mapped between DB names <-> API names """
    # Reverse map: DB names => API names
    map_db_to_api: dict[str, str]
    
    # Forward map: API names => DB names
    map_api_to_db: dict[str, str]
    
    # Fields to skip: both forward and reverse
    skip_fields: set[str] = dataclasses.field(default_factory=set)

    # Fields to fail upon: both forward and reverse
    fail_fields: set[str] = dataclasses.field(default_factory=set)

    def api_to_db(self, name: str, context: FieldContext) -> Optional[str]:
        if name in self.skip_fields:
            return None
        elif name in self.fail_fields:
            raise UnknownFieldError(name)
        
        return self.map_api_to_db[name]
    
    def db_to_api(self, name: str) -> Optional[str]:
        if name in self.skip_fields:
            return None
        elif name in self.fail_fields:
            raise UnknownFieldError(name)
        
        return self.map_db_to_api[name]
    
    def update(self, fields_map: FieldsMap):
        self.map_db_to_api.update(fields_map.map_db_to_api)
        self.map_api_to_db.update(fields_map.map_api_to_db)
        self.skip_fields.update(fields_map.skip_fields)
        self.fail_fields.update(fields_map.fail_fields)
        return self


def map_dict(db_to_api_map: dict[str, str], *, skip: abc.Iterable[str] = (), fail: abc.ITerable[str] = ()) -> FieldsMap:
    """ Initialize a FieldMap from a dictionary, bi-directional 
    
    Args:
        db_to_api_map: The mapping of { db names => api names }
    """
    return FieldsMap(
        map_db_to_api=db_to_api_map.copy(),
        map_api_to_db={v: k for k, v in db_to_api_map.items()},
        skip_fields=set(skip),
        fail_fields=set(fail),
    )


def map_db_fields_list(field_names: abc.Iterable[str], db_to_api: abc.Callable[[str], str], *, skip: abc.Iterable[str] = (), fail: abc.Iterable[str] = ()) -> FieldsMap:
    """ Given a list of DB field names, and a function, map them into API field names """
    db_to_api_map = {
        db_name: db_to_api(db_name)
        for db_name in field_names
    }
    return map_dict(db_to_api_map, skip=skip, fail=fail)


def map_api_fields_list(field_names: abc.Iterable[str], api_to_db: abc.Callable[[str], str], *, skip: abc.Iterable[str] = (), fail: abc.Iterable[str] = ()) -> FieldsMap:
    """ Given a list of API field names, and a function, map them into DB field names """
    db_to_api_map = {
        api_to_db(api_name): api_name
        for api_name in field_names
    }
    return map_dict(db_to_api_map, skip=skip, fail=fail)


def map_sqlalchemy_model(Model: type, db_to_api: abc.Callable[[str], str], *, skip: abc.Iterable[str] = (), fail: abc.Iterable[str] = ()) -> FieldsMap:
    """ Rename map from a SqlAlchemy model and a function """
    mapper = sa.orm.class_mapper(Model)
    field_names = {
        *mapper.all_orm_descriptors.keys(),
        *sainfo.properties.get_all_model_properties(Model).keys(),
    }
    return map_db_fields_list(field_names, db_to_api, skip=skip, fail=fail)


try: import graphql
except ImportError: 
    map_graphql_type = None
else:
    def map_graphql_type(type_: Union[graphql.GraphQLObjectType, graphql.GraphQLInputObjectType], api_to_db: abc.Callable[[str], str], *, skip: abc.Iterable[str] = (), fail: abc.Iterable[str] = ()) -> FieldsMap:
        """ Rename map from a GraphQL type and a function """
        field_names = type_.fields.keys()
        return map_api_fields_list(field_names, api_to_db, skip=skip, fail=fail)
