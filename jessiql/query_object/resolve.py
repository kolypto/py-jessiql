""" Tools to resolve Query Object to actual field handlers

When a QueryObject is created, it just remembers field names, but does not yet know
how to resolve them to real column names. When a Query Object is "resolved",
it gets a "handler" implementation for every field that it mentions. These handlers
implement actual ways to use a column
"""

from __future__ import annotations

from functools import singledispatch

from jessiql import sainfo
from jessiql.typing import SAModelOrAlias
from jessiql.operations import fields

from .query_object import QueryObject
from .select import SelectQuery, SelectedField, SelectedRelation
from .sort import SortQuery, SortingField
from .filter import FilterQuery, FieldFilterExpression, BooleanFilterExpression


# region Resolve operations' inputs


@singledispatch
def resolve(_, Model: SAModelOrAlias):
    """ Resolve input: a Query Object, or any of its operation's inputs

   This operation, given a specific Model class or aliased class, gathers additional information
   for actual columns and fields.

    Supports:
    * Query Object
    * Select operation
    * Sort operation
    * Filter operation
    """
    raise NotImplementedError(_)


@resolve.register
def resolve_query_object(query: QueryObject, Model: SAModelOrAlias):
    # Resolve every operation
    resolve_select(query.select, Model)
    resolve_sort(query.sort, Model)
    resolve_filter(query.filter, Model)



@resolve.register
def resolve_select(select: SelectQuery, Model: SAModelOrAlias):
    # Resolve fields
    for field in select.fields.values():
        resolve_selected_field(field, Model)

    # Resolve relations
    for relation in select.relations.values():
        resolve_selected_relation(relation, Model)


@resolve.register
def resolve_selected_field(field: SelectedField, Model: SAModelOrAlias):
    field.handler = fields.choose_selectable_handler_or_fail(field.name, None, Model)


@resolve.register
def resolve_selected_relation(field: SelectedRelation, Model: SAModelOrAlias):
    # Get the attribute
    attribute = sainfo.relations.resolve_relation_by_name(field.name, Model, where='join')

    # Populate the missing fields
    field.property = attribute.property
    assert field.property.uselist is not None  # initialized and properly configured
    field.uselist = field.property.uselist


@resolve.register
def resolve_sort(sort: SortQuery, Model: SAModelOrAlias):
    # Resolve every sorting field
    for field in sort.fields:
        resolve_sorting_field(field, Model)


@resolve.register
def resolve_sorting_field(field: SortingField, Model: SAModelOrAlias):
    field.handler = fields.choose_sortable_handler_or_fail(field.name, field.sub_path, Model)


@resolve.register
def resolve_filter(filter: FilterQuery, Model: SAModelOrAlias):
    # Resolve every filtering condition
    for condition in filter.conditions:
        resolve(condition, Model)


@resolve.register
def resolve_filtering_boolean_expression(expression: BooleanFilterExpression, Model: SAModelOrAlias):
    # Iterate expressions, resolve them
    # Use `resolve_input_element()` because it might be a filter or a boolean expression
    for clause in expression.clauses:
        resolve(clause, Model)


@resolve.register
def resolve_filtering_field_expression(expression: FieldFilterExpression, Model: SAModelOrAlias):
    expression.handler = fields.choose_filterable_handler_or_fail(expression.field, expression.sub_path, Model)
