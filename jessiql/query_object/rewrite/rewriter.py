from __future__ import annotations

from collections import abc
from typing import Union, Optional

from .base import FieldRenamer, FieldContext, UnknownFieldError


class Rewriter(FieldRenamer):
    """ Rewrites fields between API names <-> DB names. Supports nesting. """
    # Rewrite map, if already initialized
    field_renamer: Optional[FieldRenamer]

    # Rewrite map initializer: a function that will return the rewrite map
    field_renamer_getter: abc.Callable[[], FieldRenamer]
    
    # Rewriters for relations
    relation_rewriters: dict[str, Union[Rewriter, abc.Callable[[], Rewriter]]]
    
    def __init__(self, field_renamer: Union[FieldsMap, abc.Callable[[], FieldRenamer]]):
        if isinstance(field_renamer, abc.Callable):
            self.field_renamer_getter = field_renamer
            self.field_renamer = None
        else:
            self.field_renamer = field_renamer
            self.field_renamer_getter = lambda: field_renamer
        
        self.relation_rewriters = {}

    def set_relation_rewriters(self, rewriters: dict[str, RewriterOrLambda]):
        """ Set a bunch of rewriters for specific relationships by names 
        
        Args:
            rewriters: Relationship names mapped to rewriters, or lambda: Rewriter()
        """
        self.relation_rewriters.update(rewriters)
        return self

    # region Singular rewriting
    
    def get_relation_rewriter(self, relation_name: str) -> Optional[Rewriter]:
        """ Get a rewriter for a nested relationship, if any """
        rewriter = self.relation_rewriters.get(relation_name, None)
        
        # Resolve lambdas
        if callable(rewriter):
            rewriter = rewriter()
        
        return rewriter

    def api_to_db(self, name: str, context: FieldContext) -> Optional[str]:
        if self.field_renamer is None:
            self.field_renamer = self.field_renamer_getter()
        
        return self.field_renamer.api_to_db(name, context)

    def db_to_api(self, name: str) -> Optional[str]:
        if self.field_renamer is None:
            self.field_renamer = self.field_renamer_getter()
        
        return self.field_renamer.db_to_api(name)
        
    # endregion

    # region Complex rewriting

    def rewrite_query_object(self, query: QueryObject) -> QueryObject:
        """ Rewrite a QueryObject's field names and return a new one """
        return QueryObject(
            select=_rewrite_query_object_select(query.select, self),
            filter=_rewrite_query_object_filter(query.filter, self),
            sort=_rewrite_query_object_sort(query.sort, self),
            skip=query.skip,
            limit=query.limit,
            before=query.before,
            after=query.after,
        )

    # endregion


RewriterOrLambda = Union[Rewriter, abc.Callable[[], Rewriter]]


# region Rewrite helpers: QueryObject

from jessiql.query_object.query_object import QueryObject
from jessiql.query_object.select import SelectQuery, SelectedField, SelectedRelation
from jessiql.query_object.sort import SortQuery, SortingField
from jessiql.query_object.filter import FilterQuery, FilterExpressionBase, FieldFilterExpression, BooleanFilterExpression


def _rewrite_query_object_select(select: SelectQuery, rewriter: Rewriter) -> SelectQuery:
    """ Rewrite: QueryObject.select """
    return SelectQuery(
        fields=_rename_select_fields(select.fields.values(), rewriter),
        relations=_rename_select_relations(select.relations.values(), rewriter),
    )

def _rewrite_query_object_filter(filter: FilterQuery, rewriter: Rewriter) -> FilterQuery:
    """ Rewrite: QueryObject.filter """
    return FilterQuery(
        conditions=list(_rename_filter_conditions(filter.conditions, rewriter))
    )

def _rewrite_query_object_sort(sort: SortQuery, rewriter: Rewriter) -> SortQuery:
    """ Rewrite: QueryObject.sort """
    return SortQuery(
        fields=list(_rename_sort_fields(sort.fields, rewriter)),
    )

def _rename_select_fields(fields: abc.Iterable[SelectedField], rewriter: Rewriter) -> abc.Iterator[SelectedField]:
    for field in fields:
        new_name = rewriter.api_to_db(field.name, FieldContext.SELECT)
        if new_name:
            yield SelectedField(  # type: ignore[call-arg]
                name=new_name,
                handler=None,  # type: ignore[arg-type]
            )

def _rename_select_relations(relations: abc.Iterable[SelectedRelation], rewriter: Rewriter) -> abc.Iterator[SelectedRelation]:
    for relation in relations:
        new_name = rewriter.api_to_db(relation.name, FieldContext.JOIN)
        if new_name:
            nested_rewriter = rewriter.get_relation_rewriter(new_name)  # `new_name` is the DB name
            yield SelectedRelation(  # type: ignore[call-arg]
                name=new_name,
                query=nested_rewriter.rewrite_query_object(relation.query) if nested_rewriter else relation.query
            )

def _rename_sort_fields(fields: abc.Iterable[SortingField], rewriter: Rewriter) -> abc.Iterator[SortingField]:
    for field in fields:
        new_name = rewriter.api_to_db(field.name, FieldContext.SORT)
        if new_name is not None:
            yield SortingField(  # type: ignore[call-arg]
                name=new_name,
                direction=field.direction,
                sub_path=field.sub_path,  # TODO: sub-path renames
                handler=None,  # type: ignore[arg-type]
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
                    handler=None,  # type: ignore[arg-type]
                )
        else:
            raise NotImplementedError

# endregion
