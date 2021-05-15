from typing import Union

import sqlalchemy as sa
import sqlalchemy.orm


# Annotation for SqlAlchemy models
SAModel = type

# Annotation for SqlAlchemy models and aliased classes
SAModelOrAlias = Union[SAModel, sa.orm.util.AliasedClass]

# Annotation for SqlAlchemy instances
SAInstance = object

# An SqlAlchemy attribute
SAAttribute = Union[sa.orm.InstrumentedAttribute, sa.orm.MapperProperty]  # type: ignore[name-defined]
