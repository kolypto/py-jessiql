from typing import Optional, NamedTuple


class PageLinks(NamedTuple):
    """ Links to the prev/next pages """
    # Link to the previous page, if available
    prev: Optional[str]

    # Link to the next page, if available
    next: Optional[str]
