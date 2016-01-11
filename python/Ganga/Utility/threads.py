from __future__ import absolute_import


def execInThread(target, args=None, kwargs=None, timeout=None,
                 waitFlag=False, lock=(None, None),
                 callBackFunc=None):

    if args is None:
        args = ()
    if kwargs is None:
        kwargs = {}

    def _child(target, args, kwargs):
        try:
            if lock[0]:
                if lock[1] is None:
                    lock[0].acquire()
                else:
                    lock[0].acquire(lock[1])
            result = target(*args, **kwargs)
        finally:
            if lock[0]:
                lock[0].release()
        if callBackFunc:
            callBackFunc(result)
        return

    if not callable(target):
        return False

    import threading
    childThread = threading.Thread(target=_child(target, args, kwargs))
    childThread.start()

# ------------------------

from .util import GenericWrapper


class SynchronisedObject(GenericWrapper):

    def __init__(self, obj, ignore=None, lock=None):
        if ignore is None:
            ignore = ()
        if lock is None:
            import threading
            lock = threading.RLock()
        GenericWrapper.__init__(self, obj, lock.acquire, lock.release, ignore)
