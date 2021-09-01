""" Cursor-based pagination

Cursor-based pagination generates a pair of "cursors":  links to the prev and next pages that just work.
It supports keyset pagination, which is much more performant that skip/limit pagination!
"""

from .query import QueryPage
from .cursors import PageLinks
