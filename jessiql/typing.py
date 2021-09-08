from typing import Union

import sqlalchemy as sa
import sqlalchemy.orm
import sqlalchemy.ext.hybrid


# Annotation for SqlAlchemy models
SAModel = type

# Annotation for SqlAlchemy models or aliased classes
SAModelOrAlias = Union[SAModel, sa.orm.util.AliasedClass]

# Annotation for SqlAlchemy instances (objects)
SAInstance = object

# Annotation for dict rows (result rows returned as dicts)
SARowDict = dict

# An SqlAlchemy attribute
# That is, the instrumented attribute you get when accessing <model>.<attribute>
SAAttribute = Union[sa.orm.attributes.InstrumentedAttribute, sa.orm.interfaces.MapperProperty]  # type: ignore[name-defined]

# Supported SqlALchemy property
saproperty = Union[property, sa.ext.hybrid.hybrid_property]
