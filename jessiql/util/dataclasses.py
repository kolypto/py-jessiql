from dataclasses import fields
from sqlalchemy.util import symbol


def dataclass_defaults(**defaults):
    """ Feed defaults to a dataclass. Useful for slotted dataclasses

    Slotted dataclasses cannot have class-level values. As a result, you can't have defaults.
    This decorator force-feeds default values to the constructor.
    """
    def wrapper(cls):
        # Check
        unk_field_names = set(defaults) - set(dataclass_field_names(cls))
        assert not unk_field_names, f'Dataclass does not have these fields: {unk_field_names}'

        # Replace __init__(), feed defaults
        original_init = cls.__init__

        def __init__(self, /, **values):
            return original_init(self, **{**defaults, **values})
        cls.__init__ = __init__

        # Done
        return cls

    return wrapper


def dataclass_notset(*names):
    """ Feed default NOTSETs to a dataclass """
    return dataclass_defaults(**{
        name: NOTSET
        for name in names
    })


def dataclass_field_names(cls):
    return tuple(
        field.name for field in fields(cls)
    )


# Marker for values not yet set
NOTSET = symbol('NOTSET')
