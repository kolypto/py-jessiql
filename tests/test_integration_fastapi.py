from typing import Optional

import pytest
from fastapi import FastAPI
from fastapi import Depends
from fastapi.testclient import TestClient

from jessiql.integration.fastapi import query_object


EMPTY_QUERY_OBJECT = dict(
    select=[],
    join={},
    sort=[],
    filter={},
    skip=None,
    limit=None,
    before=None,
    after=None,
)


def query(**fields):
    return {
        **EMPTY_QUERY_OBJECT,
        **fields
    }


@pytest.mark.parametrize(('uri_params', 'expected_result'), [
    ('?select=["a","b","c"]', query(select=['a', 'b', 'c'])),
    ('?select=["a",{"rel":{"select":["x","y"]}}]', query(select=['a'], join={'rel': query(select=['x', 'y'])})),
    ('?sort=["a","b%2B","c-"]', query(sort=['a+', 'b+', 'c-'])),
    ('?filter={"age":{"$gt":18}}', query(filter={'age': {'$gt': 18}})),
    ('?skip=1&limit=2', query(skip=1, limit=2)),
])
def test_query_object_parameter(app: FastAPI, client: TestClient, uri_params: str, expected_result: Optional[dict]):
    """ FastAPI: get Query Object as URL parameters """
    # Prepare an API endpoint
    @app.get('/api')
    def api(query_object=Depends(query_object)):
        return {'q': query_object.dict() if query_object else None}

    # Query Object: Empty
    res = client.request('GET', '/api')
    assert res.json() == {'q': None}

    # Query Object: select
    res = client.request('GET', f'/api{uri_params}')
    assert res.json() == {'q': expected_result}


@pytest.fixture()
def app() -> FastAPI:
    return FastAPI()


@pytest.fixture()
def client(app: FastAPI) -> TestClient:
    with TestClient(app) as c:
        yield c
