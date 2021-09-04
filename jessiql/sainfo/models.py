import sqlalchemy as sa
import sqlalchemy.orm

from jessiql.typing import SAModelOrAlias


def unaliased_class(Model: SAModelOrAlias) -> type:
    """ Get the actual model class; unaliased, if was

    Args:
         Model: model class or AliasedClass
    """
    return sa.orm.class_mapper(Model).class_
