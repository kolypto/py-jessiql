from typing import Union, Optional

import graphql

from .defs import FieldQueryFunc, FieldQueryRenameFunc, FieldQueryInfo


class FieldQuery:
    """ Apply transformers and resolvers to a field name

    It allows to build a chain:
    * Functions that convert field names, e.g. to camelCase
    * Functions that return FieldQueryInfo
    and caches results.
    """
    def __init__(self, *funcs: Union[FieldQueryFunc, FieldQueryRenameFunc]):
        self.funcs = funcs
        self._cache: dict[tuple, FieldQueryInfo] = {}

    def __call__(self, field_name: str, field_def: graphql.GraphQLField, path: tuple[str, ...]) -> Optional[FieldQueryInfo]:
        # Cached?
        cache_key = (field_name, path)
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Generate
        ret = self.get_field_info(field_name, field_def, path)
        if ret is not None:
            self._cache[cache_key] = ret
        return ret

    def get_field_info(self, field_name: str, field_def: graphql.GraphQLField, path: tuple[str, ...]) -> Optional[FieldQueryInfo]:
        for func in self.funcs:
            ret = func(field_name, field_def, path)

            # Got field info? we're done :)
            if isinstance(ret, FieldQueryInfo):
                return ret
            # Got a string? That's a rename!
            elif isinstance(ret, str):
                field_name = ret
                continue
            # Got None? Keep going; this guy knows nothing
            elif ret is None:
                continue
            # Oops
            else:
                raise NotImplementedError
        else:
            # Nobody knows nothing
            return None
