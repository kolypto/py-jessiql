""" Different ways of skipping fields """

from typing import Optional

from .base import RewriterBase, FieldContext, SkipField, UnknownFieldError




class Skip(RewriterBase):
    """ Skip field: signal the field to be skipped

    Next handler will not be invoked because we send a terminal SkipField signal.
    """
    def api_to_db(self, name: str, context: FieldContext) -> Optional[str]:
        raise SkipField

    def db_to_api(self, name: str) -> Optional[str]:
        raise SkipField


class Ignore(RewriterBase):
    """ Ignore fields

    This fallback handler that will just give `None` to every field, giving way to any subsequent handler.
    This default behavior is already in place, but perhaps, you'll want to be explicit.
    """
    def api_to_db(self, name: str, context: FieldContext) -> Optional[str]:
        return None

    def db_to_api(self, name: str) -> Optional[str]:
        return None


class Fail(RewriterBase):
    """ Fail every field

    This fallback handler will give UnknownFieldError for every field that it encounters
    """
    def api_to_db(self, name: str, context: FieldContext) -> Optional[str]:
        raise UnknownFieldError(name)

    def db_to_api(self, name: str) -> Optional[str]:
        raise UnknownFieldError(name)
