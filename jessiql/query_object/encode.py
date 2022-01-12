import json

from .query_object import QueryObjectDict


def query_object_param(query_object: QueryObjectDict = None, **query_object_dict) -> dict[str, str]:
    """ Encode a Query Object for request params: stringify complex values """
    return {
        name: json.dumps(value)
        for name, value in {**(query_object or {}), **query_object_dict}.items()
    }
