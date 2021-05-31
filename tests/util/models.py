import sqlalchemy as sa

# Manyfields: a helper/mixin to have many fields at once


class ManyFieldsMixin:
    """ A mixin with many columns """
    a = sa.Column(sa.String)
    b = sa.Column(sa.String)
    c = sa.Column(sa.String)
    d = sa.Column(sa.String)


def manyfields(prefix: str, n: int):
    """ Make a dict for a ManyFields object

    Example:
        User(
            id=1,
            **manyfields('user', 1),
        )
        => User(id=1, a='user-1-a', b='user-1-b', c='user-1-c', d='user-1-d')
    """
    return {
        k: f'{prefix}-{n}-{k}'
        for k in 'abcd'
    }


class IdManyFieldsMixin(ManyFieldsMixin):
    """ A mixin with many columns and an id primary key """
    id = sa.Column(sa.Integer, primary_key=True)


def id_manyfields(prefix: str, id: int, **extra):
    """ Make a dict for a ManyFields object that also has an id

    Example:
        User(**id_manyfields('user', 1, email='kolypto@gmail.com'))
        => User(id=1, a='user-1-a', b='user-1-b', c='user-1-c', d='user-1-d', email='kolypto@gmail.com')
    """
    return {
        'id': id,
        **manyfields(prefix, id),
        **extra
    }
