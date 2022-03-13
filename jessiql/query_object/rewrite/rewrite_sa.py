from jessiql import sainfo
import sqlalchemy as sa
import sqlalchemy.orm

from .rewrite import Rewriter, RewriterBase
from .rewrite import _rename_select_fields, _rename_select_relations, SelectedField, SelectQuery


class RewriteSAModel(Rewriter):
    """ Rewrite SqlAlchemy model fields.

    Note that it will NOT discard unknown fields!! Use rewrite.Skip() explicitly to achieve this behavior.

    This class knows the difference between columns and relationships:
    if a complex column (e.g. JSON) is selected as "join" (because the UI does not know the difference),
    it will move it into "select".
    """

    def __init__(self, *rules: RewriterBase, Model: type):
        super().__init__(*rules)
        self.Model = Model
        self.mapper: sa.orm.Mapper = sa.orm.class_mapper(Model)

    def _rewrite_query_object_select(self, select: SelectQuery) -> SelectQuery:
        # Rewrite
        select = super()._rewrite_query_object_select(select)

        # Go through the relationships, move non-relationships to columns
        # Why: because the API user might not know that some nested objects are not relationships.
        for name in select.relations.keys():
            attr = self.mapper.all_orm_descriptors.get(name)
            if not attr or not sainfo.relations.is_relation(attr):
                select.relations.pop(name)
                select.fields[name] = SelectedField(name=name)  # type: ignore[call-arg]

        # Done
        return select
