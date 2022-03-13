from re import A

import pytest
from jessiql import QuerySettings, QueryObject
from jessiql.query_object import rewrite


def test_query_object_rewrite():
    """ Test Query Object rewriting """

    # === Test: Rename
    rewriter = rewrite.Rewriter(
        rewrite.Rename({
            'userName': 'user_name',
            'userAge': 'user_age',
        }),
    )

    api_query = QueryObject.from_query_object({
        # Test: select rewrites
        'select': ['userName', 'userAge'],
        # Test: sort rewrites
        'sort': ['userName+', 'userAge-'],
        # Test: filter rewrites
        'filter': {
            '$or': [
                {'userName': 'A'},
                {'userName': 'B'},
            ],
            'userAge': {'$gt': 18},
        },
    })
    query = rewriter.query_object(api_query)
    assert query.dict() == query_object(
        select=['user_name', 'user_age'],
        sort=['user_name+', 'user_age-'],
        filter={
            '$or': [
                {'user_name': {'$eq': 'A'}},
                {'user_name': {'$eq': 'B'}},
            ],
            'user_age': {'$gt': 18},
        }
    )


    # === Test: Transform
    to_lower = rewrite.Transform(
        lambda name: name.lower(),
        lambda name: name,
    )

    rewriter = rewrite.Rewriter(to_lower)
    api_query = QueryObject.from_query_object({'select': ['userName', 'userAge']})
    assert rewriter.query_object(api_query).dict() == query_object(select=['username', 'userage'])


    # === Test: skip.Ignore
    rewriter = rewrite.Rewriter(
        rewrite.Rename({})
    )
    api_query = QueryObject.from_query_object({
        # This field will just be ignored
        'select': ['unknownField'],
    })
    query = rewriter.query_object(api_query)
    assert query.dict() == query_object()  # empty


    # === Test: skip.Fail
    rewriter = rewrite.Rewriter(
        rewrite.Fail()
    )
    api_query = QueryObject.from_query_object({
        # This field will just be ignored
        'select': ['unknownField'],
    })
    with pytest.raises(rewrite.UnknownFieldError):
        rewriter.query_object(api_query)


    # === Test: nesting
    qsets = QuerySettings(
        rewriter=rewrite.Rewriter(to_lower),
        relations={
            'articles': QuerySettings(
                rewriter=rewrite.Rewriter(to_lower),
            )
        }
    )
    assert qsets.rewriter.settings is qsets  # Linked

    api_query = QueryObject.from_query_object({
        'select': ['userName'],
        'join': {
            'articles': {
                'select': ['articleId'],
            }
        },
    })
    query = qsets.rewriter.query_object(api_query)
    assert query.dict() == query_object(
        select=['username'],
        join={
            'articles': query_object(
                select=['articleid'],
            ),
        }
    )


    # === Test: RewriteSAModel
    # this test is implemented in: test_integration_graphql.py


def query_object(select=[], sort=[], filter={}, join={}, skip=None, limit=None) -> dict:
    return {'select': select, 'sort': sort, 'filter': filter, 'join': join, 'skip': skip, 'limit': limit}
