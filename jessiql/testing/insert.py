import sqlalchemy as sa


def insert(connection: sa.engine.Connection, Model: sa.sql.Selectable, *values):
    stmt = sa.insert(Model).values(*values)
    connection.execute(stmt)
