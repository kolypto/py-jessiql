__version__ = __import__('pkg_resources').get_distribution('jessiql').version

from .engine import Query
from .query_object import QueryObject, QueryObjectDict

from . import query_object
from . import exc

from .features.cursor import QueryPage, PageLinks


# TODO: README.md

# TODO: a tool to apply JessiQL Query Object to plain lists & dicts (to fake behavior for non-JessiQL APIs).
#   To achieve that, implement a Query-like engine object that processes Python objects

# TODO: pluck JSON & JSONB attribute with dot-notation in 'select'?
#   > { select: ["meta.id"] }
# TODO: support @property and @hybrid_property
#   This includes: select, sort, filter
# TODO: filter by related objects
#   > { filter: { "user.articles.id": 10 } }
# TODO: aggregation
#   > min, max, sum, avg + filter
# TODO: pagination URL generation for (skip,limit) and for akeyset pagination
#   > generate URLs, generate akeyset conditions

# TODO: use baked queries in JSelectInLoader to speed things up

# TODO: support other databases (MySQL?) See (tag:postgres-only)
