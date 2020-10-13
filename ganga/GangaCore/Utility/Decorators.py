import functools
import time

from GangaCore.Utility.logging import getLogger

logger = getLogger()


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


def repeat_while_none(max=5, first=0.1, multiplier=2, message='Waiting'):
    '''
    Decorator to call function multiple times until it returns a not-None
    value. It will be called a maximum of 'max' times, will wait for
    'first' seconds before the first call, and then an escalating amount for
    each subsequent call (each 'multiplier' longer)
    
    @repeat_while_none(4, 0.5, 5)
    def test():
        return None

    will result in 4 calls to the function with wait times of 0.5s, 5s,
    10s, 15s and will then eventually return None.
    '''
    def real_repeat(func):
        """
        Repeats execution upto 'max' times
        """
        @functools.wraps(func)
        def wrapper_repeat(*args, **kwargs):
            time.sleep(first)
            for i in range(max):
                ret = func(*args, **kwargs)
                if ret is not None:
                    break
                logger.info(message)
                time.sleep(multiplier*i)
            return ret
        return wrapper_repeat
    return real_repeat
