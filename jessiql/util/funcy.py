from functools import wraps


# Borrowed from: funcy
def collecting(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        return list(
            func(*args, **kwargs)
        )
    return wrapper
