from functools import wraps


# Borrowed from: funcy
def collecting(func):
    """ Convert a generator to a list-returning function

    Example:
        @collecting
        def count():
            yield 1
            yield 2
            yield 3
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        return list(
            func(*args, **kwargs)
        )
    return wrapper
