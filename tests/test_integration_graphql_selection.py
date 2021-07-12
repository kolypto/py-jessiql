from typing import Union, Optional

import graphql
import pytest
from contextlib import nullcontext as does_not_raise

from jessiql.integration.graphql import (
    selected_field_names,
    selected_field_names_naive,
    selected_field_names_from_info,
    selected_fields_tree,
)

@pytest.mark.parametrize(('query', 'runtime_type', 'expected_fields', 'expectation'), [
    # Field selection
    ('{ id }', None, ['id'], does_not_raise()),
    ('{ id name value }', None, ['id', 'name', 'value'], does_not_raise()),
    ('{ field(arg: "hey") }', None, ['field'], does_not_raise()),
    # Nested selection
    ('{ id object { id name } }', None, ['id', 'object'], does_not_raise()),
    # Alias
    # NOTE: every field is named as in the schema, not as the alias says!
    ('''{ alias: field(arg: "hey") 
          alias: field { a b } }''', None,
     ['field', 'field'], does_not_raise()),
    # Directive
    ('{ field @something }', None, ['field'], does_not_raise()),
    # Fragment spread
    (''' {
          hero(episode: "EMPIRE") {
            ...fragmentName
          }
     }''',
     None,
     ['hero'], does_not_raise()
     ),
    # Inline fragment without `runtime_type`
    (''' { ...fragmentName }
        fragment fragmentName on Object {
          id
          object { id name }
     }''',
     None,
     [], pytest.raises(RuntimeError)
     ),
    # Inline fragment
    (''' { ...fragmentName }
        fragment fragmentName on Object {
          id
          object { id name }
        }
    ''',
     'Object',
     ['id', 'object'], does_not_raise()
     ),
    # Inline fragment
    (''' {
        name
        ... on Droid {
          primaryFunction
        }
        ... on Human {
          height
        }
     }''',
     'Droid',
     ['name', 'primaryFunction'], does_not_raise()
     )
])
def test_selection(query: str, runtime_type: Optional[str], expected_fields: list[str], expectation):
    """ Test selected_field_names() and selected_field_names_naive() against all sorts of queries, in plain text """
    context = graphql_execution_context_for_query(query)

    # Test: selected_field_names()
    with expectation:
        actual_fields = list(selected_field_names(
            context.schema,
            context.fragments,
            context.variable_values,
            context.operation.selection_set,
            runtime_type=runtime_type
        ))
        assert actual_fields == expected_fields

    # Test: selected_field_names_naive()
    if runtime_type is not None:
        expectation = pytest.raises(RuntimeError)

    with expectation:
        actual_fields = list(selected_field_names_naive(
            context.operation.selection_set,
        ))
        assert actual_fields == expected_fields


@pytest.mark.parametrize(('query', 'runtime_type', 'expected_result'), [
    # Flat case
    ('query {  id name }', None, {
        'id': None, 'name': None,  # bug: resolver is not called
        # 'id': '1',
        # 'name': 'id name',  # list of selected fields at this level
    }),
    # Nested case
    ('query {  id name object { id name object { name } } }', None, {
        'id': None, 'name': None,  # bug: resolver is not called
        # 'id': '1',
        # 'name': 'id name object',
        'object': {
            'id': '1',
            'name': 'id name object',
            'object': {
                'name': 'name',
            },
        }
    }),
])
def test_selection_in_resolver(query: str, runtime_type: Optional[str], expected_result: dict):
    """ Test selected_field_names() when used in a resolver function """
    # GraphQL resolver
    def resolve_object(obj, info: graphql.GraphQLResolveInfo):
        names = selected_field_names_from_info(info, runtime_type=runtime_type)
        return {'id': 1, 'name': ' '.join(sorted(names))}  # return as a name

    # Prepare our schema
    schema = graphql.build_schema(GQL_SCHEMA)

    # Bind resolver
    schema.type_map['Query'].fields['object'].resolve = resolve_object
    schema.type_map['Object'].fields['object'].resolve = resolve_object

    # Execute
    res = graphql.graphql_sync(schema, query)
    assert not res.errors
    assert res.data == expected_result


@pytest.mark.skip('Benchmark')
def test_selection_benchmark(N_ITER=10 ** 6):
    import logging
    logging.root.setLevel(logging.INFO)

    # This is the query we're going to parse
    document = '''
    {
        id name value 
        object { id name } 
        field(arg: "a") 
        hero(episode: "a")
    }
    '''

    # Prepare context
    context = graphql_execution_context_for_query(document)

    # Benchmark
    from jessiql.testing.profile import timeit

    with timeit('selected_field_names_naive()'):
        for i in range(N_ITER):
            selected_field_names_naive(context.operation.selection_set)

    with timeit('selected_field_names()'):
        for i in range(N_ITER):
            selected_field_names(
                context.schema,
                context.fragments,
                context.variable_values,
                context.operation.selection_set,
                runtime_type='Object'
            )

    pytest.fail(pytrace=False)



@pytest.mark.parametrize(('query', 'runtime_type', 'expected_fields', 'expectation'), [
    # Field selection
    ('{ id name value }', None, ['id', 'name', 'value'], does_not_raise()),
    ('{ field(arg: "hey") }', None, ['field'], does_not_raise()),
    # Nested selection
    ('{ id object { id name } }', None, ['id', {'object': ['id', 'name']}], does_not_raise()),
    ('{ id object { id name object { id name } } }', None, ['id', {'object': ['id', 'name', {'object': ['id', 'name']}]}], does_not_raise()),
])
def test_selection_tree(query: str, runtime_type: Optional[str], expected_fields: dict, expectation):
    context = graphql_execution_context_for_query(query)

    # Test: selected_field_names()
    with expectation:
        actual_fields = list(selected_fields_tree(
            context.schema,
            context.fragments,
            context.variable_values,
            context.operation.selection_set,
            runtime_type=runtime_type
        ))
        assert actual_fields == expected_fields


# language=graphql
GQL_SCHEMA = '''
    type Query {
        id: ID
        name: String
        value: String
        object: Object
        field(arg: String): String
        hero(episode: String): String
    }
    
    type Object {
        id: ID
        name: String
        object: Object  # nested object
    }
    
    # Droid: to be used as `... on Droid` inline fragment
    type Droid {
        primaryFunction: String
    }
    
    # Droid: to be used as `... on Human` inline fragment
    type Human {
        height: Int
    }
'''


def graphql_execution_context_for_query(source: str) -> graphql.ExecutionContext:
    """ Parse a gql'query{ fields }' and return an ExecutionContext

    This context is what selected_field_names() needs.

    You can get the compiled schema with `context.schema`.
    """
    # Parse the document
    document = graphql.parse(source)

    # Build the context
    schema = graphql.build_schema(GQL_SCHEMA)
    context = graphql.ExecutionContext.build(schema, document)

    # Done
    return context


