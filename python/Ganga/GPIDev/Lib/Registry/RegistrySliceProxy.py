from __future__ import absolute_import

from Ganga.GPIDev.Base.Proxy import stripProxy, implRef
from Ganga.Utility.logging import getLogger

class RegistrySliceProxy(object):

    """This object is an access list to registry slices"""

    def __init__(self, impl):
        setattr(self, implRef, impl)

    def ids(self, minid=None, maxid=None):
        """ Return a list of ids of all objects.
        """
        return stripProxy(self).ids(minid, maxid)

    def clean(self, confirm=False, force=False):
        """ Cleans this registry completely.
        Returns True on success, False on failure"""
        return stripProxy(self).clean(confirm, force)

    def incomplete_ids(self):
        try:
            return stripProxy(self).objects._incomplete_objects
        except Exception as x:
            return []

    def __iter__(self):
        """ Looping. Example:
        for j in jobs:
          print(j.id)
        """
        class Iterator(object):

            def __init__(self, reg):
                self.it = stripProxy(reg).__iter__()

            def __iter__(self): return self

            def next(self):
                return _wrap(next(self.it))
        return Iterator(self)

    def __contains__(self, j):
        return stripProxy(self).__contains__(stripProxy(j))

    def __len__(self):
        return stripProxy(self).__len__()

    def __call__(self, arg):
        return _wrap(stripProxy(self).__call__(arg))

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
        logger = getLogger()
        logger.debug("Calling: %s" % str(stripProxy(self).select))
        return self.__class__(stripProxy(self).select(minid, maxid, **unwrap_attrs))

    def _display(self, interactive=True):
        return stripProxy(self)._display(interactive)

    def __str__(self):
        return self._display(interactive=0)

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text('registry...')
            return
        p.text(self._display())


# wrap Proxy around a ganga object (or a list of ganga objects)
# leave all others unchanged
from Ganga.GPIDev.Base.Proxy import addProxy
from Ganga.GPIDev.Base.Objects import GangaObject
from .RegistrySlice import RegistrySlice

from Ganga.GPIDev.Base.Proxy import isType

def _wrap(obj):
    if isType(obj, GangaObject):
        return addProxy(obj)
    if isType(obj, RegistrySlice):
        return obj._proxyClass(obj)
    if isType(obj, list):
        return map(addProxy, obj)
    return obj

# strip Proxy and get into the ganga object implementation

def _unwrap(obj):
    return stripProxy(obj)

