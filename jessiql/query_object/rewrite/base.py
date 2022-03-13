from __future__ import annotations
from collections import abc

from enum import Enum
from typing import Any, Optional


class RewriterBase:
    """ Base class for rewrite rules """

    def api_to_db(self, name: str, context: FieldContext) -> Optional[str]:
        """ Convert API name to DB name

        Args:
            name: The field name you've gotten from the user
            context: The context the field is referred in

        Returns:
            Converted name, or `None` if this handler cannot rename the field
        Raises:
            SkipField: do not query this field from the database
        """
        raise NotImplementedError

    def db_to_api(self, name: str) -> Optional[str]:
        """ Convert DB name to API name

        Args:
            name: The field name you've gotten from the database

        Returns:
            Converted name, or `None` if the handler cannot rename the field
        Raises:
            SkipField: do not include this field into the result set
        """
        raise NotImplementedError


class FieldContext(Enum):
    """ The context in which a particular field has been referred to """
    SELECT = 'select'
    JOIN = 'join'
    SORT = 'sort'
    FILTER = 'filter'


class SkipField(Exception):
    """ Signalling exception: skip this field """
    __slots__ = ()


class UnknownFieldError(KeyError):
    """ Field name not known """


def evaluate_rules(rules: abc.Sequence[RewriterBase], method_name: str, *args) -> Any:
    """ Given a list of rules, execute `method_name(*args)` on them until a result is found

    Go through the rules in order:

    1. If a rule returns a value, return it. Stop iteration.
    2. If a rule raises SkipField, return None. Stop iteration.
    3. If no rule returned anything useful, return None
    """
    # Execute every rule in order
    for rule in rules:
        method = getattr(rule, method_name)

        # Execute the method
        try:
            result = method(*args)
        # Skip signal: no more processing
        except SkipField:
            break

        # No result from this rule: keep going
        if result is None:
            continue
        # Otherwise: return result
        else:
            return result
    # No rule was able to do anything: no result
    else:
        return None