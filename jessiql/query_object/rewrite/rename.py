from collections import abc
from typing import Optional

from .base import RewriterBase, FieldContext


class Rename(RewriterBase):
    """ Rename fields according to a map """

    # Forward map: API names => DB names
    fmap: dict[str, str]

    # Reverse map: DB names => API names
    rmap: dict[str, str]

    def __init__(self, renames: dict[str, str]):
        """

        Args:
            renames: Map of fields { API name => DB name }
        """
        self.map = renames.copy()
        self.rmap = {v: k for k, v in self.map.items()}

    def api_to_db(self, name: str, context: FieldContext) -> Optional[str]:
        return self.map.get(name)

    def db_to_api(self, name: str) -> Optional[str]:
        return self.rmap.get(name)


class KeepName(RewriterBase):
    """ Keep names for the listed fields. Do not rename. """
    # Field names to keep
    names: frozenset[str]

    def __init__(self, names: abc.Iterable[str]):
        self.names = frozenset(names)

    def api_to_db(self, name: str, context: FieldContext) -> Optional[str]:
        return name if name in self.names else None

    def db_to_api(self, name: str) -> Optional[str]:
        return name if name in self.names else None
