import pytest
from jessiql import QueryObject
from jessiql.query_object import rewrite


def test_query_object_rewrite():
    """ Test Query Object rewriting """

    # === Test: map_dict()
    rewriter = rewrite.Rewriter(
        rewrite.map_dict({
            'user_name': 'userName',
            'user_age': 'userAge',
        })
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
    query = rewriter.rewrite_query_object(api_query)
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

    # === Test: map_api_fields_list()
    rewriter = rewrite.Rewriter(
        rewrite.map_api_fields_list(
            ['userName', 'userAge'],
            str.lower,
            skip=['unknownField'],
            fail=['badField'],
        ),
    )
    api_query = QueryObject.from_query_object({'select': ['userName', 'userAge']})
    assert rewriter.rewrite_query_object(api_query).dict() == query_object(select=['username', 'userage'])


    # === Test: skip, fail
    rewriter = rewriter  # use the same one

    api_query = QueryObject.from_query_object({
        # This field will just be ignored
        'select': ['unknownField'],
    })
    query = rewriter.rewrite_query_object(api_query)
    assert query.dict() == query_object()  # empty

    api_query = QueryObject.from_query_object({
        'select': ['badField'],
    })
    with pytest.raises(rewrite.UnknownFieldError):
        rewriter.rewrite_query_object(api_query)


    # === Test: nesting
    user_rewriter = rewrite.Rewriter(lambda: rewrite.map_dict({
        # Field names
        'user_name': 'userName',
        # Relation names. Also mentioned!
        'articles': 'articles',
    })).set_relation_rewriters({
        'articles': lambda: article_rewriter,
    })

    article_rewriter = rewrite.Rewriter(lambda: rewrite.map_dict({
        'article_id': 'articleId',
        'author': 'author',
    })).set_relation_rewriters({
        'author': lambda: user_rewriter,
    })

    api_query = QueryObject.from_query_object({
        'select': ['userName'],
        'join': {
            'articles': {
                'select': ['articleId'],
                'join': {
                    'author': {
                        'select': ['userName'],
                    }
                }
            }
        },
    })

    query = user_rewriter.rewrite_query_object(api_query)
    assert query.dict() == query_object(
        select=['user_name'],
        join={
            'articles': query_object(
                select=['article_id'],
                join={
                    'author': query_object(
                        select=['user_name'],
                    )
                }
            ),
        }
    )


    # === Test: RewriteSAModel
    # this test is implemented in: test_integration_graphql.py::test_query_object_with_sa_model
    pass


    # === Test: sub-fields (dot-notation)
    rewriter = rewrite.Rewriter(
        rewrite.map_dict({
            'meta_data': 'metaData',
            'meta_data.eye_color': 'metaData.eyeColor',
            'meta_data.favorite_drink': 'metaData.favoriteDrink',
        })
    )
    api_query = QueryObject.from_query_object({
        'select': ['metaData'],
        'filter': {
            'metaData.eyeColor': 'blue',
            'metaData.favoriteDrink': 'tea',
        },
        'sort': ['metaData.eyeColor+']
    })
    query = rewriter.rewrite_query_object(api_query)
    assert query.dict() == query_object(
        select=['meta_data'],
        filter={
            # Renamed because present in the mapping
            'meta_data.eye_color': {'$eq': 'blue'},
            # Not renamed because not present in the mapping
            'meta_data.favorite_drink': {'$eq': 'tea'},
        },
        sort=['meta_data.eye_color+'],
    )



def query_object(select=[], sort=[], filter={}, join={}, skip=None, limit=None, before=None, after=None) -> dict:
    return {'select': select, 'sort': sort, 'filter': filter, 'join': join, 'skip': skip, 'limit': limit, 'before': before, 'after': after}
