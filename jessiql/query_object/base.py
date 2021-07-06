from typing import Any


class OperationInputBase:
    """ Base class for Operations' Inputs

    Operation's input is basically a parsed field of a Query Object
    """

    @classmethod
    def from_query_object(cls) -> Any:
        """ Convert the input value into an object """
        raise NotImplementedError

    def export(self) -> Any:
        """ Export the input back into some jsonable value """
        raise NotImplementedError
