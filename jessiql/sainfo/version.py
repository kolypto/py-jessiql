""" SqlAlchemy version tools """

from sqlalchemy import __version__ as SA_VERSION

# SqlAlchemy version tuple
SA_VERSION_TUPLE: tuple[int, ...] = tuple(map(int, SA_VERSION.split('.')))

# SqlAlchemy minor version: 1.X or 2.X
SA_VERSION_MINOR: tuple[int, int] = SA_VERSION_TUPLE[:2]  # type: ignore

# SqlAlchemy version bools
SA_13 = SA_VERSION_MINOR == (1, 3)
SA_14 = SA_VERSION_MINOR == (1, 4)
SA_20 = SA_VERSION_MINOR == (2, 0)
