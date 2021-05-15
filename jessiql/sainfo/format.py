import sqlalchemy as sa
import sqlalchemy.orm


def model_name(Model: type) -> str:
    """ Get the name of the Model for this class """
    return Model.__mapper__.class_.__name__
