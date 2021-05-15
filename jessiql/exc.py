
class BaseJessiqlException(AssertionError):  # `AssertionError` for backwards-compatibility
    pass


class QueryObjectError(BaseJessiqlException):
    """ Invalid input provided by the User """

    def __init__(self, err: str):
        super().__init__(f'Query object error: {err}')


class InvalidColumnError(BaseJessiqlException):
    """ Query mentioned an invalid column name """

    def __init__(self, model: str, column_name: str, where: str):
        self.model = model
        self.column_name = column_name
        self.where = where

        super().__init__(f'Invalid column "{column_name}" for "{model}" specified in {where}')


class InvalidRelationError(InvalidColumnError):
    """ Query mentioned an invalid relationship name """



class RuntimeQueryError(BaseJessiqlException):
    """ Uncaught error while processing a query

    This class is used to augment other errors
    """
