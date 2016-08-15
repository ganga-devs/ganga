from __future__ import absolute_import

from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem
from Ganga.GPIDev.Lib.GangaList.GangaList import makeGangaList

from Ganga.Core import GangaException

from Ganga.GPIDev.Base.Proxy import stripProxy

from Ganga.Core.GangaRepository.Registry import Registry, RegistryKeyError

from .RegistrySlice import RegistrySlice, config

from .RegistrySliceProxy import RegistrySliceProxy, _wrap, _unwrap

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

class BoxTypeError(GangaException, TypeError):

    def __init__(self, what=''):
        GangaException.__init__(self, what)
        self.what = what

    def __str__(self):
        return "BoxTypeError: %s" % self.what

class BoxMetadataObject(GangaObject):

    """Internal object to store names"""
    _schema = Schema(Version(1, 0), {"name": SimpleItem(
        defvalue="", copyable=1, doc='the name of this object', typelist=[str])})
    _name = "BoxMetadataObject"
    _category = "internal"
    _enable_plugin = True
    _hidden = 1


class BoxRegistry(Registry):

    def __init__(self, name, doc):
        super(BoxRegistry, self).__init__(name, doc)

        self.stored_slice = BoxRegistrySlice(self.name)
        self.stored_slice.objects = self
        self.stored_proxy = BoxRegistrySliceProxy(self.stored_slice)
        self.stored_proxy.add = self.proxy_add
        self.stored_proxy.rename = self.proxy_rename
        self.stored_proxy.remove = self.proxy_remove

    def _setName(self, obj, name):
        nobj = self.metadata[self.find(obj)]
        obj._getSessionLock()
        nobj._getSessionLock()
        nobj.name = name
        nobj._setDirty()
        obj._setDirty()

    def _getName(self, obj):
        nobj = self.metadata[self.find(obj)]
        return nobj.name

    def _remove(self, obj, auto_removed=0):
        nobj = self.metadata[self.find(obj)]
        super(BoxRegistry, self)._remove(obj, auto_removed)
        self.metadata._remove(nobj, auto_removed)

    def getIndexCache(self, obj):
        cached_values = ['status', 'id', 'name']
        c = {}
        for cv in cached_values:
            try:
                c[cv] = getattr(obj, cv)
            except AttributeError as err:
                c[cv] = None
        this_slice = BoxRegistrySlice("tmp")
        for dpv in this_slice._display_columns:
            c["display:" + dpv] = this_slice._get_display_value(obj, dpv)
        return c

# Methods for the "box" proxy (but not for slice proxies)
    def _get_obj(self, obj_id):
        if isinstance(obj_id, str):
            return self[self._getIDByName(obj_id)]
        elif isinstance(obj_id, int):
            return self[obj_id]
        else:
            obj = _unwrap(obj_id)
            return self[self.find(obj)]

    def proxy_add(self, obj, name):
        """
        Add an object to the box

        The object must also be given a descriptive text name, for example:

        box.add(Job(),'A job')

        or

        a=Executable()
        box.add(a, 'An executable application')
        """
        obj = _unwrap(obj)
        if isinstance(obj, list):
            obj = makeGangaList(obj)
        if not isinstance(obj, GangaObject):
            raise BoxTypeError(
                "The Box can only contain Ganga Objects (i.e. Applications, Datasets or Backends). Check that the object is first in box.add(obj,'name')")

        if obj._category == 'jobs':
            if hasattr(obj.application, 'is_prepared'):
                if obj.application.is_prepared is not None and obj.application.is_prepared is not True:
                    logger.debug(
                        'Adding a prepared job to the box and increasing the shareref counter')
                    obj.application.incrementShareCounter(
                        obj.application.is_prepared.name)
        if obj._category == 'applications':
            if hasattr(obj, 'is_prepared'):
                if obj.is_prepared is not None and obj.is_prepared is not True:
                    logger.debug('Adding a prepared application to the box and increasing the shareref counter')
                    obj.incrementShareCounter(obj.is_prepared.name)

        obj = obj.clone()
        nobj = BoxMetadataObject()
        nobj.name = name
        self._add(obj)
        self.metadata._add(nobj, self.find(obj))

    def proxy_rename(self, obj_id, name):
        """
        Rename an object in the box. For example:

        box(0,'new name')
        """
        self._setName(self._get_obj(obj_id), name)

    def proxy_remove(self, obj_id):

        obj = self._get_obj(obj_id)
        if obj._category == 'jobs':
            if hasattr(obj.application, 'is_prepared'):
                if obj.application.is_prepared is not None and obj.application.is_prepared is not True:
                    logger.debug(
                        'Removing a prepared job from the box and decreasing the shareref counter')
                    obj.application.decrementShareCounter(
                        obj.application.is_prepared.name)
        if obj._category == 'applications':
            if hasattr(obj, 'is_prepared'):
                if obj.is_prepared is not None and obj.is_prepared is not True:
                    logger.debug(
                        'Removing a prepared application from the box and decreasing the shareref counter')
                    obj.decrementShareCounter(obj.is_prepared.name)

        self._remove(self._get_obj(obj_id))

    def getSlice(self):
        return self.stored_slice

    def getProxy(self):
        return self.stored_proxy

    def startup(self):
        self._needs_metadata = True
        super(BoxRegistry, self).startup()


class BoxRegistrySlice(RegistrySlice):

    def __init__(self, name):
        super(BoxRegistrySlice, self).__init__(name, display_prefix="box")
        self._display_columns_functions["id"] = lambda obj: obj._getRegistry().find(obj)
        self._display_columns_functions["type"] = lambda obj: obj._name
        self._display_columns_functions["name"] = lambda obj: obj._getRegistry()._getName(obj)
        from Ganga.Utility.ColourText import Foreground, Background, Effects
        fg = Foreground()
        fx = Effects()
        bg = Background()
        self.fx = fx
        self.status_colours = {'default': fx.normal,
                               'backends': fg.orange,
                               'applications': fg.green,
                               'jobs': fg.blue}
        self._proxyClass = BoxRegistrySliceProxy

    def _getColour(self, _obj):
        try:
            return self.status_colours.get(stripProxy(_obj)._category, self.fx.normal)
        except AttributeError as err:
            return self.status_colours['default']

    def __getitem__(self, id):
        if isinstance(id, str):
            for o in self.objects:
                if o._getRegistry()._getName(o) == id:
                    return o
            raise RegistryKeyError("No object with name '%s' found in the box!" % id)
        else:
            return super(BoxRegistrySlice, self).__getitem__(id)


class BoxRegistrySliceProxy(RegistrySliceProxy):

    """This object is a list of objects in the box.

    Any Ganga object can be stored in the box. For example, a job can be added thus:

    a=Job()
    box.add(a, 'Some descriptive text')

    or an application:

    a=Executable()
    box.add(a, 'An application')

    Box objects are referenced by their IDs which can be viewed by simply calling 'box', or
    box.ids()

    Once defined, box objects can be renamed:        
    box.rename(0, 'new name')

    removed:
    box.remove(0)

    or selected:
    box.select(0)
    box.select(application='Executable')
    box.select(name='text name')


    Finally, to remove all box objects:
    box.remove_all()

    or to completelty clean the box registry:
    box.clean()
    """

    def __call__(self, x):
        """ Access individual object. Examples:
        box(10) : get object with id 10 or raise exception if it does not exist.
        """
        return _wrap(stripProxy(self).__call__(x))

    def __getitem__(self, x):
        """ Get an item by positional index. Examples:
        box[-1] : get last object,
        box[0] : get first object,
        box[1] : get second object.
        """
        return _wrap(stripProxy(self).__getitem__(x))

    def remove_all(self):
        """
        Remove all objects from the box registry.
        """

        items = stripProxy(self).objects.items()
        for id, obj in items:
            reg = obj._getRegistry()
            if not reg is None:
                reg._remove(obj)

