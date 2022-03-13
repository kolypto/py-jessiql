from collections import abc
from typing import Optional

from pydantic import Field

from .base import RewriterBase, FieldContext


class Transform(RewriterBase):
    """ Rewrite field names with a function """

    def __init__(self,
                 api_to_db: abc.Callable[[str], str],
                 db_to_api: abc.Callable[[str], str] = None):
        """

        Args:
            api_to_db: Function to convert field names from API style to DB style
            db_to_api: Function to convert field names from DB style to API style
        """
        self.map_fn = api_to_db
        self.rmap_fn = db_to_api

    def api_to_db(self, name: str, context: FieldContext) -> Optional[str]:
        return self.map_fn(name)

    def db_to_api(self, name: str) -> Optional[str]:
        assert self.rmap_fn is not None, 'Cannot do reverse rewriting: `db_to_api` func was not provided'
        return self.rmap_fn(name)
