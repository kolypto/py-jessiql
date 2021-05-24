from sqlalchemy.util import symbol


def dataclass_defaults(**defaults):
    """ Feed defaults to a dataclass. Useful for slotted dataclasses

    Slotted dataclasses cannot have class-level values. As a result, you can't have defaults.
    This decorator force-feeds default values to the constructor.
    """
    def wrapper(cls):
        # Replace __init__(), feed defaults
        original_init = cls.__init__

        def __init__(self, /, **values):
            return original_init(self, **{**defaults, **values})
        cls.__init__ = __init__

        # Done
        return cls

    return wrapper


# Marker for values not yet set
NOTSET = symbol('NOTSET')
