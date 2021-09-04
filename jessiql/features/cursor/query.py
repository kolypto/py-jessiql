from jessiql import Query

from .cursors import PageLinks
from .skiplimit import CursorLimitOperation


class QueryPage(Query):
    """ A Query that will generate cursors to navigate to the next/previous pages """
    SkipLimitOperation = CursorLimitOperation

    def page_links(self) -> PageLinks:
        """ Get links to the previous and next page

        These values are opaque cursors that you can feed to "limit" to get to the corresponding page
        """
        return self.skiplimit_op.get_page_links()  # type: ignore[attr-defined]
