import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

import jessiql
from jessiql import QueryObjectDict
from jessiql.sainfo.version import SA_14
from jessiql.testing.table_data import insert
from jessiql.testing.recreate_tables import created_tables
from jessiql.util import sacompat
from jessiql.operations.pager.cursor_skip import SkipCursorData
from jessiql.operations.pager.util import decode_opaque_cursor

from .util.models import IdManyFieldsMixin, id_manyfields
from .util.test_queries import typical_test_sql_query_text, typical_test_query_results, typical_test_query_text_and_results


@pytest.mark.parametrize(('query_object', 'expected_query_lines',), [
    # Empty: feed `None`
    (dict(skip=None), []),
    (dict(limit=None), []),
    # Values: skip 1, limit 1, skip 1 limit 1
    (dict(skip=1), ['LIMIT ALL OFFSET 0']),  # `skip` does not work without a `limit`:wa
    (dict(limit=1), ['LIMIT 2']),  # LIMIT 1 + 1 extra row for cursor pagination
    (dict(skip=1, limit=1), ['LIMIT 2 OFFSET 1']),
])
def test_skiplimit_sql(connection: sa.engine.Connection, query_object: QueryObjectDict, expected_query_lines: list[str]):
    """ Typical test: what SQL is generated """
    # Models
    Base = sacompat.declarative_base()

    class Model(IdManyFieldsMixin, Base):
        __tablename__ = 'a'

        # This Postgres-specific implementation has .contains() and .overlaps() implementations
        tags = sa.Column(pg.ARRAY(sa.String))

    # Test
    typical_test_sql_query_text(query_object, Model, expected_query_lines)


@pytest.mark.parametrize(('query_object', 'expected_results'), [
    # Empty input
    (dict(), [{'id': n} for n in (1, 2, 3)]),
    # skip 1, limit 1, skip 1 limit 1
    # use `sort` to make sure the ordering is predictable
    (dict(sort=['id'], skip=1), [{'id': n} for n in (1, 2, 3,)]),
    (dict(sort=['id'], limit=1), [{'id': n} for n in (1,)]),
    (dict(sort=['id'], skip=1, limit=1), [{'id': n} for n in (2,)]),
])
def test_skiplimit_results(connection: sa.engine.Connection, query_object: QueryObjectDict, expected_results: list[dict]):
    """ Typical test: real data, real query, real results """
    # Models
    Base = sacompat.declarative_base()

    class Model(IdManyFieldsMixin, Base):
        __tablename__ = 'a'

    # Data
    with created_tables(connection, Base):
        # Insert some rows
        insert(connection, Model,
            id_manyfields('m', 1),
            id_manyfields('m', 2),
            id_manyfields('m', 3),
        )

        # Test
        typical_test_query_results(connection, query_object, Model, expected_results)


@pytest.mark.parametrize(('query_object', 'expected_query_lines', 'expected_results'), [
    (dict(select=[{'articles': dict(sort=['id'], limit=1)}]), [
        # LIMIT through a window function
        'SELECT anon_1.user_id, anon_1.id',
        'FROM (',
            'SELECT a.user_id AS user_id, a.id AS id, row_number() OVER (PARTITION BY a.user_id ORDER BY a.id ASC NULLS LAST) AS __group_row_n',
            'FROM a',
            'WHERE a.user_id IN ([POSTCOMPILE_primary_keys])' if SA_14 else
            'WHERE a.user_id IN ([EXPANDING_primary_keys])',
            'ORDER BY a.id ASC NULLS LAST) AS anon_1',
        ')',
        'WHERE __group_row_n <= 1'
    ], [
        {'id': 1, 'articles': [
            {'id': 1, 'user_id': 1},
            # no more rows
        ]}
    ]),
    (dict(select=[{'articles': dict(sort=['id'], skip=1, limit=1)}]), [
        # still a window function
        'WHERE __group_row_n > 1 AND __group_row_n <= 2'
    ], [
         {'id': 1, 'articles': [
             # first row skipped
             {'id': 2, 'user_id': 1},
             # no more rows
         ]}
     ]),
    (dict(select=[{'articles': dict(sort=['id'], skip=1)}]), [
        # still a window function
        'WHERE __group_row_n > 1'
    ], [
        {'id': 1, 'articles': [
            # first row skipped
            {'id': 2, 'user_id': 1},
            {'id': 3, 'user_id': 1},
        ]}
    ]),
])
def test_joined_skiplimit(connection: sa.engine.Connection, query_object: QueryObjectDict, expected_query_lines: list[str], expected_results: list[dict]):
    """ Typical test: JOINs, SQL and results """
    # Models
    Base = sacompat.declarative_base()

    class User(IdManyFieldsMixin, Base):
        __tablename__ = 'u'

        articles = sa.orm.relationship('Article', back_populates='author')

    class Article(IdManyFieldsMixin, Base):
        __tablename__ = 'a'

        user_id = sa.Column(sa.ForeignKey(User.id))
        author = sa.orm.relationship(User, back_populates='articles')

    # Data
    with created_tables(connection, Base):
        # Insert some rows
        insert(connection, User,
            id_manyfields('u', 1),
        )
        insert(connection, Article,
            id_manyfields('a', 1, user_id=1),
            id_manyfields('a', 2, user_id=1),
            id_manyfields('a', 3, user_id=1),
        )

        # Test
        typical_test_query_text_and_results(connection, query_object, User, expected_query_lines, expected_results)


def test_skiplimit_cursor_pagination(connection: sa.engine.Connection):
    """ Test pagination with cursors """
    def main():
        # ### Test: wrong usage
        # Test: cannot get a link before results are fetched
        q = jessiql.Query(dict(limit=2), User)

        # Not possible to generate links before results are fetched
        with pytest.raises(RuntimeError):
            q.page_links()

        # Fetch results. Now possible.
        q.fetchall(connection)
        q.page_links()  # no error

        # ### Test: skip/limit pages

        # Test: Page 0
        # No prev page, have next page
        q, res = load(select=['id'], sort=['a'], limit=2)

        assert ids(res) == [1, 2]
        assert decode_links(q.page_links()) == (None,
                                                dict(skip=2, limit=2))

        # Test: next page
        # Have both prev & next pages
        q, res = load(select=['id'], sort=['a'], after=q.page_links().next)

        assert ids(res) == [3, 4]
        assert decode_links(q.page_links()) == (dict(skip=0, limit=2),
                                                dict(skip=4, limit=2))

        # Test: next page (last page)
        q, res = load(select=['id'], sort=['a'], after=q.page_links().next)

        assert ids(res) == [5]
        assert decode_links(q.page_links()) == (dict(skip=2, limit=2),
                                                None)

        # Test: prev page
        q, res = load(select=['id'], sort=['a'], before=q.page_links().prev)

        assert ids(res) == [3, 4]
        assert decode_links(q.page_links()) == (dict(skip=0, limit=2),
                                                dict(skip=4, limit=2))


        # Test: prev page (first)
        q, res = load(select=['id'], sort=['a'], before=q.page_links().prev)

        assert ids(res) == [1, 2]
        assert decode_links(q.page_links()) == (None,
                                                dict(skip=2, limit=2))

        # ### Test: approaching the end
        # Because this is the end, there should be no next page

        # Case 1. Got no rows => No next page.
        cursor = SkipCursorData(5, 2).encode()
        q, res = load(select=['id'], sort=['a'], after=cursor)
        assert ids(res) == []
        assert q.page_links().next is None

        # Case 2: Got one row, result set incomplete => No next page.
        cursor = SkipCursorData(4, 2).encode()
        q, res = load(select=['id'], sort=['a'], after=cursor)
        assert ids(res) == [5]
        assert q.page_links().next is None

        # Case 3: Got two rows, but there's nothing beyond that => No next page.
        cursor = SkipCursorData(3, 2).encode()
        q, res = load(select=['id'], sort=['a'], after=cursor)
        assert ids(res) == [4, 5]
        assert q.page_links().next is None

        # ### Test: keyset

        # Test: first page
        q, res = load(select=['id', 'a'], sort=['a', 'id'], limit=2)

        assert ids(res) == [1, 2]
        assert decode_links(q.page_links()) == (None,
                                                dict(cols=['a', 'id'], limit=2, op='>', val=['u-2-a', 2]))

        # Test: next page
        q, res = load(select=['id', 'a'], sort=['a', 'id'], after=q.page_links().next)

        assert ids(res) == [3, 4]
        assert decode_links(q.page_links()) == (dict(cols=['a', 'id'], limit=2, op='<', val=['u-3-a', 3]),
                                                dict(cols=['a', 'id'], limit=2, op='>', val=['u-4-a', 4]))

        # Test: next page
        q, res = load(select=['id', 'a'], sort=['a', 'id'], after=q.page_links().next)

        assert ids(res) == [5]
        assert decode_links(q.page_links()) == (dict(cols=['a', 'id'], limit=2, op='<', val=['u-5-a', 5]),
                                                None)

        # Test: prev page
        q, res = load(select=['id', 'a'], sort=['a', 'id'], before=q.page_links().prev)

        # assert ids(res) == [3, 4]  # TODO: backward navigation does not work yet
        # assert decode_links(q.page_links()) == (dict(cols=['a', 'id'], limit=2, op='<', val=['u-3-a', 3]),
        #                                         dict(cols=['a', 'id'], limit=2, op='>', val=['u-4-a', 4]))

        # ### Test: related pagination
        q, res = load(
            select=['id'],
            join={
                'articles': {
                    'select': ['id'],
                    'skip': 1,
                    'limit': 1,
                    'sort': ['id+'],
                }
            },
            filter={'id': 1}
        )
        assert res == [
            {
                'id': 1,
                'articles': [
                    # {'user_id': 1, 'id': 1},
                    {'user_id': 1, 'id': 2},
                    # {'user_id': 1, 'id': 3},
                ],
            }
        ]



    # Models
    Base = sacompat.declarative_base()

    class User(IdManyFieldsMixin, Base):
        __tablename__ = 'u'

        articles = sa.orm.relationship('Article', back_populates='author')

    class Article(IdManyFieldsMixin, Base):
        __tablename__ = 'a'

        user_id = sa.Column(sa.ForeignKey(User.id))
        author = sa.orm.relationship(User, back_populates='articles')

    # Helpers
    def load(**query_object) -> tuple[jessiql.Query, list[dict]]:
        """ Given a Query Object, load results, return (Query, result) """
        q = jessiql.Query(query_object, User)
        res = q.fetchall(connection)
        return q, res

    def ids(row_dicts: list[dict]) -> list[id]:
        """ Convert a list of dicts to ids """
        return [row['id'] for row in row_dicts]

    # Data
    with created_tables(connection, Base):
        # Insert some rows
        insert(connection, User,
            *(id_manyfields('u', id) for id in range(1, 6))
        )
        insert(connection, Article,
            id_manyfields('a', 1, user_id=1),
            id_manyfields('a', 2, user_id=1),
            id_manyfields('a', 3, user_id=1),
        )

        # Test
        main()


def decode_links(links: jessiql.PageLinks) -> tuple[dict, dict]:
    return (
        decode_opaque_cursor(links.prev)[1] if links.prev else None,
        decode_opaque_cursor(links.next)[1] if links.next else None,
    )
