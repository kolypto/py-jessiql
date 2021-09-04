""" SqlAlchemy tools """

import sqlalchemy.orm
from sqlalchemy import *
from sqlalchemy import orm

try:
    # SA 1.4
    from sqlalchemy.orm import declarative_base
except ImportError:
    # SA 1.3
    from sqlalchemy.ext.declarative import declarative_base
    sqlalchemy.orm.declarative_base = declarative_base
