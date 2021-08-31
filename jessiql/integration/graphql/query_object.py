from typing import Union, Any, Optional

import graphql

from jessiql import QueryObjectDict, QueryObject

from .query_object_input_name import get_query_argument_name_for
from .selection import collect_fields
from .query_field_func import QueryFieldFunc, query_field_default, QueryFieldInfo


def query_object_for(info: graphql.GraphQLResolveInfo, runtime_type: Union[str, graphql.GraphQLObjectType] = None) -> QueryObject:
    """ Inspect the GraphQL query and make a Query Object Dict.

    Args:
        info: The `info` object from your field resolver function
        runtime_type: The name for the model you're currently querying. Used to resolve fragments that depend on types.
            If you need to resolve multiple types, call this function multiple times to get different Query Objects.

    Example:
        def resolve_user(obj, info):
            names = query_object_from_info(info, 'User')
    """
    assert len(info.field_nodes) == 1  # I've never seen a selection of > 1 field
    field_node = info.field_nodes[0]

    # Get the Query Object dict
    selected_field_def = graphql.utilities.type_info.get_field_def(info.schema, info.parent_type, field_node)
    query_object_dict = graphql_query_object_dict_from_query(
            info.schema,
            info.fragments,
            info.variable_values,
            selected_field_def=selected_field_def,
            selected_field=field_node,
            runtime_type=runtime_type
        )

    # Convert into a QueryObject
    return QueryObject.from_query_object(query_object_dict)


def graphql_query_object_dict_from_query(
        schema: graphql.GraphQLSchema,
        fragments: dict[str, graphql.FragmentDefinitionNode],
        variable_values: dict[str, Any],
        selected_field_def: graphql.GraphQLField,
        selected_field: graphql.FieldNode, *,
        runtime_type: Union[str, graphql.GraphQLObjectType] = None,
        field_query: QueryFieldFunc = query_field_default,
        _field_query_path: tuple[str] = (),
) -> QueryObjectDict:
    """ Inspect the GraphQL query and make a Query Object Dict.

    1. Traverses the GraphQL tree
    2. Finds every field that has a JessiQL `query` argument.
       It can have any name; it is identified by its type: "QueryObjectInput" (defined as a constant: QUERY_OBJECT_INPUT_NAME)
    3. These fields are included as "join" relations.
    4. Some other fields are included as "select" fields.

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
        field_query: A function to decide how a particular field should be included into the Query Object

    Returns:
        Query Object Dict.

    Raises:
        KeyError: a type is not found by name
        RuntimeError: fragment was used, but `runtime_type` was not provided
    """
    # Get the query argument name and the Query Object Input
    query_arg_name = get_query_argument_name_for(selected_field_def)
    assert query_arg_name is not None  # Make sure this function is only called on fields that actually have a query
    query_arg = get_query_argument_value_for(selected_field, query_arg_name, variable_values) or {}

    # Prepare the Query Object Dict we're going to return
    query_object = {
        'select': [],
        'join': {},
        **query_arg
    }

    # Collect all fields at this level
    fields = collect_fields(schema, fragments, variable_values, selected_field.selection_set, runtime_type=runtime_type)
    selected_field_type: graphql.type.definition.GraphQLObjectType = unwrap_type(selected_field_def.type)  # unwrap lists & nonnulls, deal with raw types

    # Iterate every field on this level, see if there's a place for them in the Query Object
    for field_name, field_list in fields.items():
        for field in field_list:
            # Get field definition from the schema
            field_def: graphql.type.definition.GraphQLField = selected_field_type.fields[field.name.value]

            # How to include this field?
            info: QueryFieldInfo = field_query(field_name, field_def, _field_query_path)

            # Select field
            if info.select:
                query_object['select'].extend(info.select)

            # Join relation
            if info.join_name:
                query_object['join'][info.join_name] = graphql_query_object_dict_from_query(
                    schema,
                    fragments,
                    variable_values,
                    field_def,
                    field,
                    # TODO: runtime type currently cannot be resolved for sub-queries. This means that fragments cannot be used.
                    #   How to fix? callable() that feeds the type? @directives?
                    runtime_type=None,
                    field_query=field_query,
                    _field_query_path=_field_query_path+(info.join_name,),
                )
            # Skip other fields
            else:
                continue  # skip field

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


unwrap_type = graphql.get_named_type  # Unwrap GraphQL wrapper types (List, NonNull, etc)
