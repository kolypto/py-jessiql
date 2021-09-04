""" Get the list of fields selected with a GraphQL query """

import graphql

from collections import abc
from typing import Union, Any, Optional


# TODO: when this solution is tested & published, post a link here:
#   https://github.com/graphql-python/graphene/issues/57


# TODO: in most cases, `runtime_type` can be inferred from the schema -- in cases when there's no union.
#   Try to use:
#   > runtime_type = get_operation_root_type(self.schema, operation)


def selected_field_names_from_info(info: graphql.GraphQLResolveInfo, runtime_type: Union[str, graphql.GraphQLObjectType] = None) -> abc.Iterator[str]:
    """ Shortcut: selected_field_names() when used in a resolve function

    Example:
        def resolve_user(obj, info):
            names = selected_field_names_from_info(info, 'User')
    """
    assert len(info.field_nodes) == 1  # I've never seen a selection of > 1 field
    field_node = info.field_nodes[0]

    return selected_field_names(
        info.schema,
        info.fragments,
        info.variable_values,
        field_node.selection_set,  # type: ignore[arg-type]
        runtime_type=runtime_type
    )


def selected_field_names(
        schema: graphql.GraphQLSchema,
        fragments: dict[str, graphql.FragmentDefinitionNode],
        variable_values: dict[str, Any],
        selection_set: graphql.SelectionSetNode, *,
        runtime_type: Union[str, graphql.GraphQLObjectType] = None) -> abc.Iterator[str]:
    """ Get the list of field names selected at the current level

    Supports:
    * fields
    * sub-queries (only returns the basic name)
    * fragment spreads (`... fragmentName`)
    * inline fragments (`... on Droid { }`)

    Does not support:
    * Directives. All fields are included.

    Args:
        schema: The GraphQL schema we query against
        fragments:
            Fragments defined in the query. Used to resolve fragments
            Typically: info.fragments
        variable_values:
            Values for the variables. Used to evaluate directives
            Typically: info.variable_values
        selection_set:
            The selected field to start traversing from.
            Typically: info.fields[0].selection_set
        runtime_type:
            The name/object of the current object type.
            Example: if your field returns Humans and Droids, you'll have two distinct set of fields: one for humans, and one for droids.
            This variable provides the name of the type you're currently interested in.

    Returns:
        Mapping { field name => FieldNode }
        Note that some fields may be aliased: in this case, "field name" would be the alias

    Raises:
        KeyError: a type is not found by name
        RuntimeError: fragment was used, but `runtime_type` was not provided

    Example:
        def resolve_user(obj, info):
            names = selected_field_names(
                info.schema,
                info.fragments,
                info.variable_values,
                info.field_nodes[0].selection_set,
                runtime_type='User',
            )

    Example:
        With a query like this:
            query {
                id
                object { id name }
                field(arg: "value")
            }
        this function would give:
            ['id', 'object', 'field']
    """
    # Collect fields
    fields_map = collect_fields(
        schema,
        fragments,
        variable_values,
        selection_set,
        runtime_type=runtime_type
    )

    # Get field names
    return (
        field.name.value  # NOTE: return the original field name, even if it's aliased
        for fields in fields_map.values()
        for field in fields
    )


def selected_field_names_naive(selection_set: graphql.SelectionSetNode) -> abc.Iterator[str]:
    """ Get the list of field names that are selected at the current level. Does not include nested names.

    Supports:
    * fields
    * sub-queries (only returns the name)

    Does not support:
    * will FAIL if a fragment is used
    * Directives. All fields are included.

    As a result:
    * It will give a RuntimeError if a fragment is used
    * It may report fields that are excluded by directives (@skip and @if)
    * It is 25x faster than selected_field_names(),
      but the full-featured version executes in ~6μs, so there actually is no reason to worry

    Args:
        selection_set:
            Selected field.
            Typically: info.fields[0].selected_set

    Example:
        With a query like this:
            query {
                id
                object { id name }
                field(arg: "value")
            }
        this function would give:
            ['id', 'object', 'field']
    """
    assert isinstance(selection_set, graphql.SelectionSetNode)

    for node in selection_set.selections:
        # Field
        if isinstance(node, graphql.FieldNode):
            # NOTE: in case of an alias, it still returns the actual field name, not the alias!
            yield node.name.value
        # Fragment spread (`... fragmentName`) and inline fragment (`... on Droid { }`)
        elif isinstance(node, (graphql.FragmentSpreadNode, graphql.InlineFragmentNode)):
            raise RuntimeError('GraphQL query contains fragments but this particular query does not support them '
                               'because a naïve parsing method is used.')
        # Something new
        else:
            raise NotImplementedError(str(type(node)))


def selected_fields_tree(
        schema: graphql.GraphQLSchema,
        fragments: dict[str, graphql.FragmentDefinitionNode],
        variable_values: dict[str, Any],
        selection_set: graphql.SelectionSetNode, *,
        runtime_type: Union[str, graphql.GraphQLObjectType] = None) -> list[Union[str, dict[str, list]]]:
    """ Get the tree of selected fields

    NOTE: this function is not really used in JessiQL. It's just an experiment.

    NOTE: `runtime_type` is only supported at this level. Fragments are therefore not supported futher down the tree.

    Example:
        Input: 'query { user { id login tags { id name } }'
        Output: [
            'id',
            'login',
            {'tags': ['id', 'name']}
        ]
    """
    fields_map = collect_fields(
        schema,
        fragments,
        variable_values,
        selection_set,
        runtime_type=runtime_type
    )

    return [
        field.name.value if not field.selection_set else {
            field.name.value: selected_fields_tree(schema, fragments, variable_values, field.selection_set, runtime_type=None)
        }
        for fields in fields_map.values()
        for field in fields
    ]


def collect_fields(
        schema: graphql.GraphQLSchema,
        fragments: dict[str, graphql.FragmentDefinitionNode],
        variable_values: dict[str, Any],
        selection_set: graphql.SelectionSetNode, *,
        runtime_type: Union[str, graphql.GraphQLObjectType] = None) -> dict[str, list[graphql.FieldNode]]:
    """ Get the list of field names that are selected at the current level.

    This low-level methods collects selected fields like this:

        Input: "query { id name }"
        Output: {
            'id': [FieldNode],
            'name': [FieldNode],
        }

    This function uses ExecutionContext.collect_fields() which is the full-featured parser used by GraphQL.
    Because ExecutionContext is never available in resolvers, we have to create our own context and evaluate the query.

    Returns:
        Mapping of { names => selected fields }
    """
    assert isinstance(fragments, dict)
    assert isinstance(variable_values, dict)
    assert isinstance(selection_set, graphql.SelectionSetNode)

    # Resolve `runtime_type`
    if isinstance(runtime_type, str):
        runtime_type = schema.type_map[runtime_type]  # type: ignore[assignment]  # raises: KeyError

    # Create a fake execution context that is just capable enough to collect fields
    # It's like a lightweight ExecutionContext that reuses its capabilities
    execution_context = GraphqlFieldCollector(
        schema=schema,
        fragments=fragments,
        variable_values=variable_values,
    )

    # Resolve all fields
    visited_fragment_names: set[str] = set()
    fields_map = execution_context.collect_fields(
        # runtime_type=runtime_type or None,
        # Use an object that would fail GraphQL internal tests
        runtime_type=runtime_type or graphql.GraphQLObjectType('<temp>', []),  # type: ignore[arg-type]
        selection_set=selection_set,
        fields={},  # (out) memo
        visited_fragment_names=visited_fragment_names, # out
    )

    # Test fragment resolution
    if visited_fragment_names and not runtime_type:
        raise RuntimeError(f'GraphQL query contains fragments but this particular query does not support them '
                           f'because object type was not specified in the code. Failed fragments: {", ".join(visited_fragment_names)}')

    # Results!
    return fields_map


class GraphqlFieldCollector(graphql.ExecutionContext):
    """ A fake ExecutionContext that only can collect_fields() """
    def __init__(
        self,
        schema: graphql.GraphQLSchema,
        fragments: dict[str, graphql.FragmentDefinitionNode],
        variable_values: dict,
    ):
        # NOTE: we intentionally do not call super() to save some unnecessary computing.
        # This class only needs to be good enough to run one method: collect_fields()
        self.schema = schema
        self.fragments = fragments
        self.variable_values = variable_values
