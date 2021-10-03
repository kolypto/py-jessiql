from typing import Optional, Union

import graphql


def resolves(schema: graphql.GraphQLSchema, type_name: str, field_name: Optional[str]):
    """ Quickly bind a resolver to a field

    Example:
        @resolves(gql_schema, 'Query', 'getUser')
        def resolve_get_user(root, info: graphql.GraphQLResolveInfo, id: int):
            ...
    """
    # Get the type
    type: Union[graphql.GraphQLObjectType, graphql.GraphQLInputObjectType] = schema.type_map[type_name]  # type: ignore[assignment]

    # Get the field (if provided)
    target: Union[graphql.GraphQLObjectType, graphql.GraphQLInputObjectType, graphql.GraphQLField]
    if not field_name:
        target = type
    else:
        target = type.fields[field_name]

    # Bind the resolver
    def decorator(func):
        target.resolve = func
        return func
    return decorator
