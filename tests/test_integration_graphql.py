import pytest
import sqlalchemy as sa

import graphql
from graphql import graphql_sync
from graphql import GraphQLResolveInfo

from jessiql import QueryObjectDict
from jessiql.integration.graphql import query_object_for

from tests.util.models import IdManyFieldsMixin


EMPTY_QUERY_OBJECT = dict(
    select=[],
    join={},
    sort=[],
    filter={},
    skip=None,
    limit=None,
)


def query(**fields):
    return {
        **EMPTY_QUERY_OBJECT,
        **fields
    }


@pytest.mark.parametrize(('query', 'variables', 'expected_result_query'), [
    # Test: no explicit JessiQL query
    (
        ''' 
        query { 
            object { id query } 
        }
        ''',
        {},
        {
            'object': {
                # NOTE: select[] includes "query": field query function does not inspect the model!
                # This naïve selector would include every field that it finds
                'id': '1', 'query': query(select=['id', 'query']),
            },
        }
    ),
    # Test: JessiQL query: "query" argument
    (
        ''' 
        query($query: QueryObjectInput) { 
            objects( query: $query ) { id query } 
        }
        ''',
        {'query': dict(limit=10, sort=['id-'])},
        {
            'objects': [{
                # The final query object: [ id query ] fields + "query" argument put together
                'id': '1', 'query': query(select=['id', 'query'], limit=10, sort=['id-']),
            }],
        }
    ),
    # Test: JessiQL nested query, no explicit "query" argument
    (
        ''' 
        query($query: QueryObjectInput) { 
            objects( query: $query ) { 
                id query
                objects { id } 
            } 
        }
        ''',
        {'query': dict(sort=['id-'])},
        {
            'objects': [{
                'id': '1', 'query': query(select=['id', 'query'], sort=['id-'], join={'objects': query(select=['id'])}),
                'objects': [{'id': '1'}],
            }]
        }
    ),
    # Test: JessiQL nested query
    (
        ''' 
        query($query: QueryObjectInput) { 
            objects { 
                id query
                objects (query: $query) { id } 
            } 
        }
        ''',
        {'query': dict(sort=['id-'])},
        {
            'objects': [{
                'id': '1',
                'query': query(
                    select=['id', 'query'],
                    join={
                        'objects': query(
                            select=['id'],
                            sort=['id-'])}),
                'objects': [{
                    'id': '1',
                }],
            }]
        }
    ),
    # Test: JessiQL nested query for "objects", multi-level
    (
    ''' query { 
        objects { 
            id query
            objects { id objects { id } } 
        } 
    }
    ''',
        {},
        {
            'objects': [{
                'id': '1',
                'query': query(
                    select=['id', 'query'],
                    join={
                        'objects': query(
                            select=['id'],
                            join={
                                'objects': query(
                                    select=['id'])})}),
                'objects': [{
                    'id': '1',
                    'objects': [{
                        'id': '1',
                    }]}],
            }]
        }
    ),
    # Test: JessiQL nested query for "object" (singular), multi-level
    (
        ''' query { 
            object { 
                id query
                object { id object { id } } 
            } 
        }
        ''',
            {},
            {
                'object': {
                    'id': '1',
                    'query': query(
                        select=['id', 'query'],
                        join={
                            'object': query(
                                select=['id'],
                                join={
                                    'object': query(
                                        select=['id'])})}),
                    'object': {
                        'id': '1',
                        'object': {
                            'id': '1',
                        }},
                }
            }
        ),
])
def test_query_object(query: str, variables: dict, expected_result_query: dict):
    # Models
    Base = sa.orm.declarative_base()

    class Model(IdManyFieldsMixin, Base):
        __tablename__ = 'models'

        # Define some relationships
        object_id = sa.Column(sa.ForeignKey('models.id'))
        object_ids = sa.Column(sa.ForeignKey('models.id'))

        object = sa.orm.relationship('Model', foreign_keys=object_id)
        objects = sa.orm.relationship('Model', foreign_keys=object_ids)

    # GraphQL resolver
    def resolve_object(obj, info: GraphQLResolveInfo, query: QueryObjectDict = None):
        query_object = query_object_for(info, runtime_type='Model')
        return {
            'id': 1,
            'query': query_object.dict(),
        }

    def resolve_objects(obj, info: GraphQLResolveInfo, query: QueryObjectDict = None):
        query_object = query_object_for(info, runtime_type='Model')
        return [
            {
                'id': 1,
                'query': query_object.dict(),
            },
        ]

    # Prepare our schema
    from jessiql.integration.graphql.schema import graphql_jessiql_schema
    schema = graphql.build_schema(
        GQL_SCHEMA +
        # Also load QueryObject and QueryObjectInput
        graphql_jessiql_schema
    )

    # Bind resolvers
    schema.type_map['Query'].fields['object'].resolve = resolve_object
    schema.type_map['Model'].fields['object'].resolve = resolve_object
    schema.type_map['Query'].fields['objects'].resolve = resolve_objects
    schema.type_map['Model'].fields['objects'].resolve = resolve_objects

    # Execute
    res = graphql_sync(schema, query, variable_values=variables)

    if res.errors:
        raise res.errors[0]
    __import__('pprint').pprint(res.data)
    assert res.data == expected_result_query


# language=graphql
GQL_SCHEMA = '''
type Query {
    object (query: QueryObjectInput): Model
    objects (query: QueryObjectInput): [Model]
}

type Model {
    # Real Model fields
    id: ID!
    a: String!
    b: String!
    c: String!
    d: String!
    
    objectId: ID
    objectIds: [ID]
    
    # Real relationships
    object (query: QueryObjectInput): Model
    objects (query: QueryObjectInput): [Model]
    
    # Virtual attribute that returns the Query Object
    query: Object
}

'''
