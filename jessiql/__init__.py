__version__ = __import__('pkg_resources').get_distribution('jessiql').version

from .engine import Query
from .query_object import QueryObject, QueryObjectDict

from . import query_object
from . import exc

# TODO: write comments for: operations, testing, sautil
# TODO: README.md
# TODO: update SqlAlchemy, see if any changes can/should be made to JSelectInLoader
# TODO: make MyPy happy

# TODO: GraphQL integration

# TODO: JSON & JSONB objects, attribute access for filter, sort, select
#   > { filter: { "meta.id": 10 } }
# TODO: support @property and @hybrid_property
#   This includes: select, sort, filter
# TODO: filter by related objects
#   > { filter: { "user.articles.id": 10 } }
# TODO: aggregation
#   > min, max, sum, avg + filter
# TODO: pagination URL generation for (skip,limit) and for akeyset pagination
#   > generate URLs, generate akeyset conditions

# TODO: use baked queries to speed things up
