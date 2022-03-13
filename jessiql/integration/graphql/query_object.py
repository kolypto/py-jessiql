""" Functions that extract a Query Object from the GraphQL query

It goes through the Query tree, and:

1. Find every `query` argument and takes the Query Object 'filter', 'sort', etc, from it
2. It walks the selected fields and adds them as Query Object 'select' or 'join'
"""

from typing import Union, Any, Optional
from collections import abc

import graphql

from jessiql import QueryObjectDict, QueryObject

from .query_object_argument import get_query_argument_name_for
from .selection import collect_fields


def query_object_for(info: graphql.GraphQLResolveInfo, nested_path: abc.Iterable[str] = (), *,
                     runtime_type: Union[str, graphql.GraphQLObjectType] = None,
                     query_argument: Optional[str] = None,
                     has_query_argument: bool = True,
                     query_object_type_name: str = None,
                     ) -> QueryObject:
    """ Inspect the GraphQL query and make a Query Object Dict.

    1. Assuming that the current selected field has the JessiQL query argument (type "QueryObjectInput")
    2. The actual object may be nested under `nested_path` (e.g. when Relay pagination is used).
    3. It traverses the GraphQL tree
    4. Finds every field that has a JessiQL `query` argument.
       It can have any name; it is identified by its type: "QueryObjectInput" (defined as a constant: QUERY_OBJECT_INPUT_NAME)
    5. These fields are included as "join" relations.
    4. Some other fields are included as "select" fields.

    Example:
        GraphQL: users(query: QueryObjectInput): [User!]
        Call this function inside the resolver

    Args:
        info: The `info` object from your field resolver function
        nested_path: Path to a sub-field where Query Object collection should start at.
            Use when Query Object fields are nested within some wrapper object.
        runtime_type: The name for the model you're currently querying. Used to resolve fragments that depend on types.
            If you need to resolve multiple types, call this function multiple times to get different Query Objects.
        query_argument: The name of the Query Object argument. Default: auto-detect by type: QueryObjectInput
        has_query_argument: Shall we attempt to get the value of the query argument?
        query_object_type_name: QueryObjectInput type name. Provide if overridden.
    Example:
        def resolve_user(obj, info):
            names = query_object_from_info(info, 'User')
    """
    assert len(info.field_nodes) == 1  # I've never seen a selection of > 1 field
    field_node = list(info.field_nodes)[0]  # we cannot do [0] directly on its type: `Collection`
    parent_type = info.parent_type

    # Get selected field definition
    selected_field_def = graphql.utilities.type_info.get_field_def(info.schema, parent_type, field_node)

    # Get the Query Object dict
    query_object_dict = graphql_query_object_dict_from_query(
            info.schema,
            info.fragments,
            info.variable_values,
            selected_field_def=selected_field_def,  # type: ignore[arg-type]
            selected_field=field_node,
            nested_path=nested_path,
            runtime_type=runtime_type,
            query_argument=query_argument,
            has_query_argument=has_query_argument,
            query_object_type_name=query_object_type_name,
        )

    # Convert into a QueryObject
    return QueryObject.from_query_object(query_object_dict)


def graphql_query_object_dict_from_query(
        schema: graphql.GraphQLSchema,
        fragments: dict[str, graphql.FragmentDefinitionNode],
        variable_values: dict[str, Any], *,
        selected_field_def: graphql.GraphQLField,
        selected_field: graphql.FieldNode,
        nested_path: abc.Iterable[str] = (),
        runtime_type: Union[str, graphql.GraphQLObjectType] = None,
        query_argument: Optional[str] = None,
        has_query_argument: bool = True,
        query_object_type_name: str = None,
) -> QueryObjectDict:
    """ Inspect the GraphQL query and make a Query Object Dict.

    Args:
         schema: GraphQL schema to use: the definition
         fragments: Query fragments: named fragments defined in the user query
         variable_values: Query variables: values for the parameters defined by the user in their query
         selected_field_def: GraphQL definition of the selected field at this level.
            Is used to get the parent data type for the top-level field selection.
         selected_field: The selected field.
            Is used to determine which sub-fields to select from this parent field.
         runtime_type: The name for the model you're currently querying. Used to resolve fragments that depend on types.
            If you need to resolve multiple types, call this function multiple times to get different Query Objects.
        query_argument: The name of the Query Object argument. Default: auto-detect by type: QueryObjectInput
        has_query_argument: Shall we attempt to get the value of the query argument?
        query_object_type_name: QueryObjectInput type name. Provide if overridden.
    Returns:
        Query Object Dict.

    Raises:
        KeyError: a type is not found by name
        RuntimeError: fragment was used, but `runtime_type` was not provided
    """
    # Get the query argument name and the Query Object Input
    if has_query_argument:
        query_arg_name = query_argument or get_query_argument_name_for(selected_field_def, query_object_type_name=query_object_type_name)
        assert query_arg_name is not None, (
            'Current field has no JessiQL Query Object argument. '
            'If this is expected, set `has_query_argument=False`.'
        )
        query_arg = get_query_argument_value_for(selected_field, query_arg_name, variable_values) or {}
    # Sometimes there might be no QueryObject input at all: e.g. mutation methods. Filter & sort make no sense with them.
    else:
        query_arg = {}

    # unwrap lists & nonnulls, deal with raw types
    selected_field_type: graphql.type.definition.GraphQLObjectType = unwrap_type(selected_field_def.type)  # type: ignore[assignment]

    # Nested path?
    # This section works in cases when `query` is on one level, but the actual object is on a lower level.
    # For instance:
    #   users(query: QueryObjectInput): UserConnection
    #   UserConnection: { edges { node: User } }
    # In this case, `selected_field` and `selected_field_def` point to `users`, but `nested_path` should descend
    # down to `node` and take User fields from there.
    try:
        selected_field, selected_field_type = descend_into_field_with(nested_path, selected_field=selected_field, field_type=selected_field_type)
    except KeyError:
        # It fails when the user gives a bad field name.
        # We will not fail. Let GraphQL fail for us.
        pass

    # Prepare the Query Object Dict we're going to return
    query_object: QueryObjectDict = {
        'select': [],
        'join': {},
        **query_arg  # type: ignore[misc]
    }

    # Collect all fields at this level
    fields = collect_fields(
        schema,
        fragments,
        variable_values,
        selected_field.selection_set,  # type: ignore[arg-type]
        runtime_type=runtime_type
    )

    # Iterate every field on this level, see if there's a place for them in the Query Object
    # Note that `field_name` may not be the original field name: it may be aliased by the query!
    for field_name, field_list in fields.items():
        for field in field_list:
            # `field_name`: field name as given by the user, possibly aliased
            # `field.name.value`: field name as defined in the schema

            # Schema field name
            schema_field_name = field.name.value

            # Get field definition from the schema
            try:
                field_def: graphql.type.definition.GraphQLField = selected_field_type.fields[schema_field_name]
            except KeyError:
                # It fails when the user gives a bad field name.
                # We will not fail. Let GraphQL fail for us.
                continue

            # How to include this field?
            # If it has a selection, then we include it as a relation -- unless it's decorated with @jessiql_select
            has_selection = field.selection_set and field.selection_set.selections
            jessiql_select = get_directive('jessiql_select', field_def.ast_node)

            if not has_selection or jessiql_select:
                query_object['select'].append(schema_field_name)  # type: ignore[union-attr]
            else:
                query_object['join'][schema_field_name] = graphql_query_object_dict_from_query(  # type: ignore[index]
                    schema,
                    fragments,
                    variable_values,
                    selected_field_def=field_def,
                    selected_field=field,
                    # TODO: runtime type currently cannot be resolved for sub-queries. This means that fragments cannot be used.
                    #   How to fix? callable() that feeds the type? @directives?
                    runtime_type=None,
                )

    return query_object



def get_query_argument_value_for(field: graphql.FieldNode, query_arg_name: str, variables: dict[str, Any]) -> Optional[dict]:
    """ Given a name, pick the Query Object argument for a field

    Args:
        query_arg_name: Name of the "query" argument of type "QueryObjectInput"
        field: Field node. Its arguments will be used.
        variables: Query variables (to resolve if referenced by name)

    Returns:
        The value of the "query" argument, or None if not provided.
        Note it will be a partial Query Object Dict: no "select" nor "join" fields are available here.
    """
    # Arguments are an array, we have to iterate and find the one with the right name
    for argument in field.arguments:
        if argument.name.value == query_arg_name:
            return graphql.value_from_ast_untyped(argument.value, variables)
    # Nothing found
    else:
        return None


def descend_into_field_with(path: abc.Iterable[str], *, selected_field: graphql.FieldNode, field_type: graphql.GraphQLObjectType):
    """ Given a selected field (query) and its type (schema), follow `path` and descend into both

    Args:
        path: List of field names to descend into
        selected_field: The field the user has selected in a query
        field_def: The field type

    Returns:
        (selected_field, field_def)
    """
    for name in path:
        if not selected_field.selection_set:
            # CHECKME: perhaps, don't fail?
            raise ValueError(f"Field {selected_field.name.value!r} has no selections. Nesting cannot descent. Is your `nested_path` correct?")

        # Descend into: selected fields
        try:
            selected_field = next(sel_node
                                  for sel_node in selected_field.selection_set.selections
                                  if (isinstance(sel_node, graphql.FieldNode) and
                                      sel_node.name.value == name and
                                      selected_field.selection_set is not None))
        except StopIteration:
            raise KeyError(selected_field)

        # Descend into: schema
        field_type = unwrap_type(field_type.fields[name].type)

    return selected_field, field_type


# copied from: apiens.tools.graphql.ast
def get_directive(directive_name: str, node: graphql.FieldDefinitionNode = None) -> Optional[graphql.DirectiveNode]:
    """ Get a directive from a field by name """
    if not node or not node.directives:
        return None

    for directive in node.directives:
        if directive.name.value == directive_name:
            return directive
    else:
        return None

unwrap_type = graphql.get_named_type  # Unwrap GraphQL wrapper types (List, NonNull, etc)
