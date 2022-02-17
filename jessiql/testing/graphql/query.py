""" Making queries with GraphQL """
from __future__ import annotations

from typing import NamedTuple, Any

import graphql


def graphql_query_sync(schema: graphql.GraphQLSchema, query: str, context_value: Any = None, **variable_values):
    """ Make a GraphqQL query, quick. Fail on errors. """
    res = graphql.graphql_sync(schema, query, variable_values=variable_values, context_value=context_value)

    # Raise errors as exceptions. Useful in unit-tests.
    if res.errors:
        # On error? raise it as it is
        if len(res.errors) == 1:
            raise res.errors[0]
        # Many errors? Raise as a list
        else:
            # TODO: In Python 3.11, raise ExceptionGroup
            raise RuntimeError(res.errors)

    return res.data


def prepare_graphql_query_for(schema_str: str, query_str: str) -> QueryContext:
    """ Given a schema and a query against it, prepare everything needed to do low-level testing

    Args:
        schema_str: GraphQL Schema string
        query_str: User query string

    Returns:
        schema: compiled schema
        query: compiled query
        execution_context: the low-level object for query execution
        info: ResolveInfo to be passed to resolvers
    """
    # Prepare the schema and the query document
    schema = graphql.build_schema(schema_str)
    query = graphql.parse(query_str)

    # Validate
    graphql.validate(schema, query)

    # Prepare ResolveInfo for the top-level object (query)
    execution_context: graphql.ExecutionContext = graphql.ExecutionContext.build(schema=schema, document=query)  # type: ignore[assignment]
    info = build_resolve_info_for(schema, query, execution_context)

    # Done
    return QueryContext(schema=schema, query=query, execution_context=execution_context, info=info)


class QueryContext(NamedTuple):
    """ Everything necessary for low-level testing of a query """
    schema: graphql.GraphQLSchema
    query: graphql.DocumentNode
    execution_context: graphql.ExecutionContext
    info: graphql.GraphQLResolveInfo


def build_resolve_info_for(schema: graphql.GraphQLSchema, query: graphql.DocumentNode, execution_context: graphql.ExecutionContext) -> graphql.GraphQLResolveInfo:
    """ Given a simple query, prepare the ResolveInfo object for the top level """
    # We only support one query in this test
    assert len(query.definitions) == 1
    query_type = schema.type_map['Query']
    query_node: graphql.ExecutableDefinitionNode = query.definitions[0]  # type: ignore[assignment]
    query_selection = query_node.selection_set.selections

    return execution_context.build_resolve_info(
        field_def=graphql.utilities.type_info.get_field_def(schema, query_type, query_selection[0]), # type: ignore[arg-type]
        field_nodes=query_selection, # type: ignore[arg-type]
        parent_type=query_type, # type: ignore[arg-type]
        path=compat_Path(None, 'query', None),
    )


def compat_Path(prev, key, typename):
    """ Compatibility for Path() """
    # Old versions: two arguments
    if graphql.version_info < (3, 1, 3):
        return graphql.pyutils.Path(prev, key)
    # New versions: three arguments
    else:
        return graphql.pyutils.Path(prev, key, typename)
