def static_vars(**kwargs):
    """
    Decorate a function and provide it with some static variables.
    >>> @static_vars(counter=0)
    ... def foo():
    ...     foo.counter += 1
    ...     return foo.counter
    >>> foo()
    1
    >>> foo()
    2
    """
    def decorate(func):
        func.__dict__.update(kwargs)
        return func
    return decorate