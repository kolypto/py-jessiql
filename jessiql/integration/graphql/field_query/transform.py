""" Transform GraphQL field names """

from collections import abc
import graphql


class RenameField:
    """ Use a callable to rename a field.

    Example: ariadne.utils.convert_to_snake_case
    """
    def __init__(self, renamer: abc.Callable[[str], str]):
        self.renamer = renamer

    def __call__(self, field_name: str, field_def: graphql.GraphQLField, path: tuple[str, ...]) -> str:
        return self.renamer(field_name)
