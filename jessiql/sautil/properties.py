from typing import Any


def evaluate_property_on_dict(prop: property, row: dict) -> Any:
    """ Given a @property, evaluate this function against a dict

    This is awfully ineffective, but enables us to use @property functions on dicts selected by JessiQL
    """
    return prop.fget(GetterDict(row))  # type: ignore[misc]


class GetterDict:
    """ A thin wrapper that makes a dict behave like an object in terms of attribute access """
    def __init__(self, d: dict):
        self.__dict = d

    __slots__ = '__dict'

    def __getattr__(self, key):
        return self.__dict[key]
