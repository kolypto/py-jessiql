""" Logical operators """

from collections import abc
from typing import Optional
from .base import RewriterBase, FieldContext, evaluate_rules


# Condition function:
#   (name, context: FieldContext) -> bool
ConditionFn = abc.Callable[[str, Optional[FieldContext]], bool]


class Condition(RewriterBase):
    """ Conditionally apply a list of rules """

    def __init__(self, condition: ConditionFn, *rules: RewriterBase, otherwise: abc.Iterable[RewriterBase] = ()):
        """

        Args:
            condition: a callable that, given a field name, returns a boolean
            rules: list of rules to execute when `condition` gave true
            otherwise: list of rules to execute when `condition` gave false
        """
        self.condition = condition
        self.rules_when_true = list(rules)
        self.rules_when_false = list(otherwise)

    def api_to_db(self, name: str, context: FieldContext) -> Optional[str]:
        rules = (
            self.rules_when_true if self.condition(name, context) else
            self.rules_when_false
        )
        return evaluate_rules(rules, 'api_to_db', name, context)

    def db_to_api(self, name: str) -> Optional[str]:
        rules = (
            self.rules_when_true if self.condition(name, None) else
            self.rules_when_false
        )
        return evaluate_rules(rules, 'api_to_db', name)


class ForFields(Condition):
    """ Apply a list of rules to specific field names """

    def __init__(self, names: abc.Iterable[str], *rules: RewriterBase, otherwise: abc.Iterable[RewriterBase] = ()):
        self.names = frozenset(names)
        super().__init__(self._condition_func, *rules, otherwise=otherwise)

    def _condition_func(self, name: str, context: FieldContext = None) -> bool:
        return name in self.names
