
class RegistrySliceProxy(object):

    """This object is an access list to registry slices"""

    def __init__(self, _impl):
        self.__dict__['_impl'] = _impl

    def ids(self, minid=None, maxid=None):
        """ Return a list of ids of all objects.
        """
        return self._impl.ids(minid, maxid)

    def clean(self, confirm=False, force=False):
        """ Cleans this registry completely.
        Returns True on success, False on failure"""
        return self._impl.clean(confirm, force)

    def incomplete_ids(self):
        try:
            return self._impl.objects._incomplete_objects
        except Exception as x:
            return []

    def __iter__(self):
        """ Looping. Example:
        for j in jobs:
          print(j.id)
        """
        class Iterator(object):

            def __init__(self, reg):
                self.it = reg._impl.__iter__()

            def __iter__(self): return self

            def next(self):
                return _wrap(next(self.it))
        return Iterator(self)

    def __contains__(self, j):
        return self._impl.__contains__(j._impl)

    def __len__(self):
        return self._impl.__len__()

    def select(self, minid=None, maxid=None, **attrs):
        """ Select a subset of objects. Examples for jobs:
        jobs.select(10): select jobs with ids higher or equal to 10;
        jobs.select(10,20) select jobs with ids in 10,20 range (inclusive);
        jobs.select(status='new') select all jobs with new status;
        jobs.select(name='some') select all jobs with some name;
        jobs.select(application='Executable') select all jobs with Executable application;
        jobs.select(backend='Local') select all jobs with Local backend.
        """
        unwrap_attrs = {}
        for a in attrs:
            unwrap_attrs[a] = _unwrap(attrs[a])
        return self.__class__(self._impl.select(minid, maxid, **unwrap_attrs))

    def _display(self, interactive=0):
        return self._impl._display(interactive)

    __str__ = _display


# wrap Proxy around a ganga object (or a list of ganga objects)
# leave all others unchanged
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from Ganga.GPIDev.Base.Objects import GangaObject
from RegistrySlice import RegistrySlice


def _wrap(obj):
    if isinstance(obj, GangaObject):
        return GPIProxyObjectFactory(obj)
    if isinstance(obj, RegistrySlice):
        return obj._proxyClass(obj)
    if type(obj) == list:
        return map(GPIProxyObjectFactory, obj)
    return obj

# strip Proxy and get into the ganga object implementation


def _unwrap(obj):
    try:
        return obj._impl
    except AttributeError:
        return obj
