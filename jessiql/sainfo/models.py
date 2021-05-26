from jessiql.typing import SAModelOrAlias


def unaliased_class(Model: SAModelOrAlias) -> type:
    """ Get the actual model class; unaliased, if was

    Args:
         Model: model class or AliasedClass
    """
    return Model.__mapper__.class_
