from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

# add display default values for the box
from RegistrySlice import config
config.addOption('box_columns',
                 ("id","type","name","application"),
                 'list of job attributes to be printed in separate columns')

config.addOption('box_columns_width',
                 {'id': 5, 'type':20, 'name':40, 'application':15},
                 'width of each column')

config.addOption('box_columns_functions',
                 {'application': "lambda obj: obj.application._name"},
                 'optional converter functions')

config.addOption('box_columns_show_empty',
                 ['id'],
                 'with exception of columns mentioned here, hide all values which evaluate to logical false (so 0,"",[],...)')

from Ganga.Core import GangaException
class BoxTypeError(GangaException,TypeError):
    def __init__(self,what):
        GangaException.__init__(self,what)
        self.what=what
    def __str__(self):
        return "BoxTypeError: %s"%self.what

from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem
from Ganga.GPIDev.Lib.GangaList.GangaList import makeGangaList

class BoxMetadataObject(GangaObject):
    """Internal object to store names"""
    _schema = Schema(Version(1,0), {"name": SimpleItem(defvalue="",copyable=1,doc='the name of this object',typelist=["str"])})
    _name   = "BoxMetadataObject"
    _category = "internal"
    _enable_plugin = True
    _hidden = 1

from Ganga.Core.GangaRepository.Registry import Registry, RegistryKeyError
class BoxRegistry(Registry):
    def _setName(self,obj,name):
        nobj = self.metadata[self.find(obj)]
        obj._getWriteAccess()
        nobj._getWriteAccess()
        nobj.name = name
        nobj._setDirty()
        obj._setDirty()

    def _getName(self,obj):
        nobj = self.metadata[self.find(obj)]
        nobj._getReadAccess()
        return nobj.name
    
    def _remove(self, obj, auto_removed=0):
        nobj = self.metadata[self.find(obj)]
        super(BoxRegistry,self)._remove(obj,auto_removed)
        self.metadata._remove(nobj,auto_removed)

    def getIndexCache(self,obj):
        cached_values = ['status','id','name']
        c = {}
        for cv in cached_values:
            if cv in obj._data:
                c[cv] = obj._data[cv]
        slice = BoxRegistrySlice("tmp")
        for dpv in slice._display_columns:
            c["display:"+dpv] = slice._get_display_value(obj, dpv)
        return c

# Methods for the "box" proxy (but not for slice proxies)
    def _get_obj(self,obj_id):
        if type(obj_id) == str:
            return self[self._getIDByName(obj_id)]
        elif type(obj_id) == int:
            return self[obj_id]
        else:
            obj = _unwrap(obj_id)
            return self[self.find(obj)]

    def proxy_add(self,obj,name):
        """
        Add an object to the box
         
        The object must also be given a descriptive text name, for example:

        box.add(Job(),'A job')

        or

        a=Executable()
        box.add(a, 'An executable application')
        """
        obj = _unwrap(obj)
        if isinstance(obj,list):
            obj = makeGangaList(obj)
        if not isinstance(obj,GangaObject):
            raise BoxTypeError("The Box can only contain Ganga Objects (i.e. Applications, Datasets or Backends). Check that the object is first in box.add(obj,'name')")

        if obj._category == 'jobs':
            if hasattr(obj.application, 'is_prepared'):
                if obj.application.is_prepared is not None and obj.application.is_prepared is not True:
                    logger.debug('Adding a prepared job to the box and increasing the shareref counter')
                    obj.application.incrementShareCounter(obj.application.is_prepared.name)
        if obj._category == 'applications':
            if hasattr(obj, 'is_prepared'):
                if obj.is_prepared is not None and obj.is_prepared is not True:
                    logger.debug('Adding a prepared application to the box and increasing the shareref counter')
                    obj.incrementShareCounter(obj.is_prepared.name)

        obj = obj.clone()
        nobj = BoxMetadataObject()
        nobj.name = name
        self._add(obj)
        self.metadata._add(nobj,self.find(obj))
        nobj._setDirty()
        obj._setDirty()

    def proxy_rename(self,obj_id,name):
        """
        Rename an object in the box. For example:
    
        box(0,'new name')
        """
        self._setName(self._get_obj(obj_id), name)
    
    def proxy_remove(self,obj_id):

        obj=self._get_obj(obj_id)
        if obj._category == 'jobs':
            if hasattr(obj.application, 'is_prepared'):
                if obj.application.is_prepared is not None and obj.application.is_prepared is not True:
                    logger.debug('Removing a prepared job from the box and decreasing the shareref counter')
                    obj.application.decrementShareCounter(obj.application.is_prepared.name)
        if obj._category == 'applications':
            if hasattr(obj, 'is_prepared'):
                if obj.is_prepared is not None and obj.is_prepared is not True:
                    logger.debug('Removing a prepared application from the box and decreasing the shareref counter')
                    obj.decrementShareCounter(obj.is_prepared.name)




        self._remove(self._get_obj(obj_id))




    def getProxy(self):
        slice = BoxRegistrySlice(self.name)
        slice.objects = self
        proxy = BoxRegistrySliceProxy(slice)
        proxy.add = self.proxy_add
        proxy.rename = self.proxy_rename
        proxy.remove = self.proxy_remove
        return proxy

    def startup(self):
        self._needs_metadata = True
        super(BoxRegistry,self).startup()

from RegistrySlice import RegistrySlice 
from Ganga.Core.GangaRepository import getRegistry
class BoxRegistrySlice(RegistrySlice):
    def __init__(self,name):
        super(BoxRegistrySlice,self).__init__(name,display_prefix="box")
        self._display_columns_functions["id"] = lambda obj : obj._getRegistry().find(obj)
        self._display_columns_functions["type"] = lambda obj : obj._name
        self._display_columns_functions["name"] = lambda obj : obj._getRegistry()._getName(obj)
        from Ganga.Utility.ColourText import Foreground, Background, Effects
        fg = Foreground()
        fx = Effects()
        bg = Background()
        self.fx = fx
        self.status_colours = { 'default'    : fx.normal,
                           'backends'    : fg.orange,
                           'applications': fg.green,
                           'jobs'        : fg.blue}
        self._proxyClass = BoxRegistrySliceProxy


    def _getColour(self,obj):
        return self.status_colours.get(obj._category,self.fx.normal)

    def __getitem__(self,id):
        if isinstance(id,str):
            matches = []
            for o in self.objects:
                if o._getRegistry()._getName(o) == id:
                    return o
                    matches.append(o)
            if len(matches) == 1:
                return matches[0]
            elif len(matches) > 1:
                raise RegistryKeyError("Multiple objects with name '%s' found in the box - use IDs!" % id) 
            else:
                raise RegistryKeyError("No object with name '%s' found in the box!" % id)
        else:
            return super(BoxRegistrySlice,self).__getitem__(id)


from RegistrySliceProxy import RegistrySliceProxy, _wrap, _unwrap
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

    def __call__(self,x):
        """ Access individual object. Examples:
        box(10) : get object with id 10 or raise exception if it does not exist.
        """
        return _wrap(self._impl.__call__(x))
                
    def __getitem__(self,x):
        """ Get an item by positional index. Examples:
        box[-1] : get last object,
        box[0] : get first object,
        box[1] : get second object.
        """
        return _wrap(self._impl.__getitem__(x))

    def __getslice__(self, i1,i2):
        """ Get a slice. Examples:
        box[2:] : get first two objects,
        box[:-10] : get last 10 objects.
        """
        return _wrap(self._impl.__getslice__(i1,i2))

    def remove_all(self):
        """
        Remove all objects from the box registry.
        """ 

        items = self._impl.objects.items()
        for id,obj in items:
            reg = obj._getRegistry()
            if not reg is None:
                reg._remove(obj)
