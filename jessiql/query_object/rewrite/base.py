from __future__ import annotations

from enum import Enum
from typing import Optional, Protocol


class FieldRenamer(Protocol):
    """ Base class for rewrite rules """

    def api_to_db(self, name: str, context: FieldContext) -> Optional[str]:
        """ Convert API name to DB name

        Args:
            name: The field name you've gotten from the user
            context: The context the field is referred in

        Returns:
            Converted name, or `None` if cannot rename the field
        
        Raises:
            UnknownFieldError: field cannot be renamed
        """
        raise NotImplementedError

    def db_to_api(self, name: str) -> Optional[str]:
        """ Convert DB name to API name

        Args:
            name: The field name you've gotten from the database

        Returns:
            Converted name, or `None` if cannot rename the field
        
        Raises:
            UnknownFieldError: field cannot be renamed
        """
        raise NotImplementedError


class UnknownFieldError(KeyError):
    """ Field name not known """


class FieldContext(Enum):
    """ The context in which a particular field has been referred to """
    SELECT = 'select'
    JOIN = 'join'
    SORT = 'sort'
    FILTER = 'filter'
