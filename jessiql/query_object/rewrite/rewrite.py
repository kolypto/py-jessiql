from __future__ import annotations

from collections import abc
from typing import Optional, Union, Any

from jessiql.engine.settings import QuerySettings
from jessiql.query_object import QueryObject
from jessiql.query_object.select import SelectQuery, SelectedField, SelectedRelation
from jessiql.query_object.sort import SortQuery, SortingField
from jessiql.query_object.filter import FilterQuery, FilterExpressionBase, FieldFilterExpression, BooleanFilterExpression
from .base import RewriterBase, evaluate_rules, FieldContext


class Rewriter:
    """ Rewrites a QueryObject from using API field names to DB fiels names """
    # Rewrite rules
    rules: list[RewriterBase]

    # Query Settings object. It's used to get rewriters for nested objects
    # This value is assigned automatically by QuerySettings when this class is linked to it.
    settings: Optional[QuerySettings]

    def __init__(self, *rules: RewriterBase):
        self.rules = list(rules)
        self.settings = None

        self.relation_rewriters: dict[Union[str, None], Rewriter] = {}

    def __copy__(self):
        return Rewriter(*self.rules)

    def query_object(self, query: QueryObject) -> QueryObject:
        """ Rewrite a QueryObject's field names and return a new one """
        return QueryObject(
            select=self._rewrite_query_object_select(query.select),
            filter=self._rewrite_query_object_filter(query.filter),
            sort=self._rewrite_query_object_sort(query.sort),
            skip=query.skip,
            limit=query.limit,
            before=query.before,
            after=query.after,
        )

    def set_relation_rewriter(self, relation_name: str = None, rewriter: Rewriter = None):
        """ Set a specific rewriter for a specific relationship

        Args:
            relation_name: The relation to set the rewriter for, or `None` to set for all
            rewriter: The rewriter to set, or `None` to use self
        """
        self.relation_rewriters[relation_name] = rewriter or self
        return self

    # Callbacks

    def api_to_db(self, name: str, context: FieldContext) -> Optional[str]:
        """ Callback: Convert API name to DB name """
        return evaluate_rules(self.rules, 'api_to_db', name, context)

    def db_to_api(self, name: str) -> Optional[str]:
        """ Callback: convert DB name to API name """
        return evaluate_rules(self.rules, 'db_to_api', name)

    def for_relation(self, relation_name: str) -> Optional[Rewriter]:
        """ Callback: get a rewriter for fields of a relationship, if any

        This will use QuerySettings, if provided.
        """
        # Get from self, if available
        rewriter = self.relation_rewriters.get(relation_name) or self.relation_rewriters.get(None)
        if rewriter is not None:
            return rewriter

        # Get it from QuerySettings, if available
        if self.settings:
            relation_settings = self.settings.get_relation_settings(relation_name)
            if relation_settings is not None:
                return relation_settings.rewriter

        # Nothing helped
        return None

    # Helpers for: query_object()

    def _rewrite_query_object_select(self, select: SelectQuery) -> SelectQuery:
        """ Rewrite: QueryObject.select """
        return SelectQuery(
            fields=_rename_select_fields(select.fields.values(), self),
            relations=_rename_select_relations(select.relations.values(), self),
        )

    def _rewrite_query_object_filter(self, filter: FilterQuery) -> FilterQuery:
        """ Rewrite: QueryObject.filter """
        return FilterQuery(
            conditions=list(_rename_filter_conditions(filter.conditions, self))
        )

    def _rewrite_query_object_sort(self, sort: SortQuery) -> SortQuery:
        """ Rewrite: QueryObject.sort """
        return SortQuery(
            fields=list(_rename_sort_fields(sort.fields, self)),
        )

# region Helpers

def _rename_select_fields(fields: abc.Iterable[SelectedField], rewriter: Rewriter) -> abc.Iterator[SelectedField]:
    for field in fields:
        new_name = rewriter.api_to_db(field.name, FieldContext.SELECT)
        if new_name:
            yield SelectedField(  # type: ignore[call-arg]
                name=new_name,
            )

def _rename_select_relations(relations: abc.Iterable[SelectedRelation], rewriter: Rewriter) -> abc.Iterator[SelectedRelation]:
    for relation in relations:
        new_name = rewriter.api_to_db(relation.name, FieldContext.JOIN)
        if new_name:
            nested_rewriter = rewriter.for_relation(new_name)  # `new_name` is the DB name
            yield SelectedRelation(  # type: ignore[call-arg]
                name=new_name,
                query=nested_rewriter.query_object(relation.query) if nested_rewriter else relation.query
            )

def _rename_sort_fields(fields: abc.Iterable[SortingField], rewriter: Rewriter) -> abc.Iterator[SortingField]:
    for field in fields:
        new_name = rewriter.api_to_db(field.name, FieldContext.SORT)
        if new_name is not None:
            yield SortingField(  # type: ignore[call-arg]
                name=new_name,
                direction=field.direction,
                sub_path=field.sub_path,  # TODO: sub-path renames
            )

def _rename_filter_conditions(conditions: abc.Iterable[FilterExpressionBase], rewriter: Rewriter) -> abc.Iterator[FilterExpressionBase]:
    for condition in conditions:
        if isinstance(condition, BooleanFilterExpression):
            yield BooleanFilterExpression(
                operator=condition.operator,
                clauses=list(_rename_filter_conditions(condition.clauses, rewriter)),
            )
        elif isinstance(condition, FieldFilterExpression):
            new_name = rewriter.api_to_db(condition.field, FieldContext.FILTER)
            if new_name is not None:
                yield FieldFilterExpression(  # type: ignore[call-arg]
                    field=new_name,
                    operator=condition.operator,
                    value=condition.value,
                    sub_path=condition.sub_path,  # TODO: sub-path renames
                )
        else:
            raise NotImplementedError

# endregion
