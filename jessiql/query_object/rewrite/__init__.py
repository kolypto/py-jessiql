""" Tools for rewriting field names for the User

For instance, your database may have a `user_id` field, but the API user would want to call it "userId".
This module rewrites field names for the Query Object, and then rewrites field names in the result set.
"""

from .base import RewriterBase, FieldContext, SkipField, UnknownFieldError
from .rewrite import Rewriter
from .rewrite_sa import RewriteSAModel

# rules
from .rename import Rename, KeepName
from .transform import Transform
from .skip import Skip, Ignore, Fail
from .bool import Condition, ForFields
