try:
    # SA 1.4
    from sqlalchemy.orm import declarative_base, DeclarativeMeta
except ImportError:
    # 1.3
    from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
