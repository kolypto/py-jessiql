""" Profiling & Benchmark """

import logging
from contextlib import contextmanager
from functools import wraps
from time import monotonic

logger = logging.getLogger(__name__)


def timeit(arg):
    """ Measure the time it takes to run a function, or a block of code.

    Can be used as a decorator, or as a context manager:

    Usage:
        with timeit('create foreign key'):
            ...

    Make sure your logger includes INFO:

        import logging
        logging.root.setLevel(logging.INFO)

    Usage:
        @timeit
        def upgrade():
            ...
    """
    if isinstance(arg, str):
        return timeit_contextmanager(arg)
    else:
        return timeit_decorator(arg)


def timeit_decorator(f):
    """ A decorator that times a function and outputs to the logger """
    @wraps(f)
    def measure_time(*args, **kwargs):
        with timeit_contextmanager(str(f)):
            return f(*args, **kwargs)
    return measure_time


@contextmanager
def timeit_contextmanager(name):
    """ A context manager that times the code and logs the result """
    t_start = monotonic()
    try:
        yield
    finally:
        t_end = monotonic()
        run_time = t_end - t_start
        logger.info(f'{name}: {run_time:.2f}s')
