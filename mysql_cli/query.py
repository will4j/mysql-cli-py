import functools


def select(sql):
    def decorator_select(func):
        @functools.wraps(func)
        def wrapper_select(*args, **kwargs):
            pass

        return wrapper_select

    return decorator_select
