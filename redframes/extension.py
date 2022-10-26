from functools import wraps
from warnings import warn


def extension(cls):
    def inner(f):
        @wraps(f)
        def wrapper(self, *args, **kwargs):
            return f(self, *args, **kwargs)

        if hasattr(cls, f.__name__):
            warn("overriding a prexisting attribute/method with the same name")
        setattr(cls, f.__name__, wrapper)
        return f

    return inner
