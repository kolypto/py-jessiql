from dataclasses import fields
from sqlalchemy.util import symbol


def dataclass_field_names(cls) -> tuple[str, ...]:
    """ Get the list of field names from a dataclass """
    return tuple(
        field.name for field in fields(cls)
    )


def dataclass_defaults(**defaults):
    """ Set defaults for a dataclass with __slots__

    Slotted dataclasses cannot have class-level values. As a result, you can't have defaults.
    This decorator force-feeds default values to the constructor.

    Example:
        @dataclass_defaults(key='DEFAULT')
        @dataclass
        class Object:
            key: str
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
    """ Set a default NOTSET value for a dataclass with __slots__

    Example:
        @dataclass_notset('key')
        @dataclass
        class Object:
            key: str
    """
    return dataclass_defaults(**{
        name: NOTSET
        for name in names
    })


# Marker for values not yet set
NOTSET = symbol('NOTSET')
