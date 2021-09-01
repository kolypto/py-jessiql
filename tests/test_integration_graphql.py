import pytest
import sqlalchemy as sa

import graphql
from graphql import graphql_sync
from graphql import GraphQLResolveInfo

from jessiql import QueryObjectDict
from jessiql.integration.graphql import query_object_for
from jessiql.integration.graphql.query_field.sa_model import QueryModelField

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
    # Test: field aliases
    (
            ''' 
            query { 
                first: object { first_id: id query } 
                second: object { second_id: id query } 
            }
            ''',
            {},
            {
                # These top-level names come from graphQL field names themselves
                'first': {
                    # The query must contain un-aliased names!
                    'first_id': '1', 'query': query(select=['id', 'query']),
                },
                'second': {
                    # The query must contain un-aliased names!
                    'second_id': '1', 'query': query(select=['id', 'query']),
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
    """ Test how Query Object is generated """
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
    schema = schema_prepare()

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


@pytest.mark.parametrize(('query_str', 'expected_query_object'), [
    # Test: Query Object only includes real fields
    (
    '''
    query {
        object { 
            # Real SA model attributes
            id a b c 
            # Do not exist on the model
            x y z query 
        }
    }
    ''',
    query(select=['id', 'a', 'b', 'c']),
    ),
    # Test: nested objects
    (
    '''
    query {
        object { 
            # 'a' is real, 'z' is not
            a z
            object {
                a z
                objects {
                    a z
                }
            } 
        }
    }
    ''',
    query(
        select=['a'],
        join={
            'object': query(
            select=['a'],
                join={
                    'objects': query(select=['a'])
                }
            ),
        }
    ),
    ),
])
def test_query_object_with_sa_model(query_str: str, expected_query_object: dict):
    """ Test how Query Object works with a real SqlAlchemy model """
    # Models
    Base = sa.orm.declarative_base()
    class Model(IdManyFieldsMixin, Base):
        __tablename__ = 'models'

        # Define some relationships
        object_id = sa.Column(sa.ForeignKey('models.id'))
        object_ids = sa.Column(sa.ForeignKey('models.id'))

        object = sa.orm.relationship('Model', foreign_keys=object_id)
        objects = sa.orm.relationship('Model', foreign_keys=object_ids)

    # Prepare the schema and the query document
    schema = schema_prepare()
    query = graphql.parse(query_str)

    # Prepare ResolveInfo for the top-level object (query)
    execution_context = graphql.ExecutionContext.build(schema=schema, document=query)
    info = build_resolve_info_for(schema, query, execution_context)

    # Get the Query Object
    query_object = query_object_for(info, runtime_type='Model', field_query=QueryModelField(Model))
    assert query_object.dict() == expected_query_object


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
    
    # Some virtual attributes that only exist in GraphQL
    x: String!
    y: String!
    z: String!
}
'''


def schema_prepare() -> graphql.GraphQLSchema:
    """ Build a GraphQL schema for testing JessiQL queries """
    from jessiql.integration.graphql.schema import graphql_jessiql_schema
    return graphql.build_schema(
        GQL_SCHEMA +
        # Also load QueryObject and QueryObjectInput
        graphql_jessiql_schema
    )


def build_resolve_info_for(schema: graphql.GraphQLSchema, query: graphql.DocumentNode, execution_context: graphql.ExecutionContext) -> graphql.GraphQLResolveInfo:
    """ Given a simple query, prepare the ResolveInfo object for the top level """
    # We only support one query in this test
    assert len(query.definitions) == 1
    query_type = schema.type_map['Query']
    query_node = query.definitions[0]
    query_selection = query_node.selection_set.selections

    return execution_context.build_resolve_info(
        field_def=graphql.utilities.type_info.get_field_def(schema, query_type, query_selection[0]),
        field_nodes=query_selection,
        parent_type=query_type,
        path=graphql.pyutils.Path(None, 'query', None)
    )
