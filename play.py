from tests.test_test import User, Post

import sqlalchemy as sa
import sqlalchemy.orm



m: sa.orm.Mapper = sa.orm.class_mapper(User)
a = sa.orm.aliased(User)
insp: sa.orm.util.AliasedInsp = sa.inspect(a)

print(repr(m))
print(vars(insp))

print(sa.select(
    a.login
))
