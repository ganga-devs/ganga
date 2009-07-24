

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


from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem

class BoxMetadataObject(GangaObject):
    """Internal object to store names"""
    _schema = Schema(Version(1,0), {"names": SimpleItem(defvalue={},copyable=1,doc='the map of ids to names of the items in the box.',typelist=["dict"])})
    _name   = "BoxMetadataObject"
    _category = "internal"
    _enable_plugin = True
    _hidden = 1
    

from Ganga.Core.GangaRepository.Registry import Registry
class BoxRegistry(Registry):
    def _createMetadataObject(self):
        return BoxMetadataObject()

    def _setName(self,obj,name):
        obj._getWriteAccess()
        self._metadata._getWriteAccess()
        self._metadata.names[self.find(obj)] = name
        self._dirty(self._metadata)
        self._dirty(obj)

    def _getName(self,obj):
        obj._getReadAccess()
        self._metadata._getReadAccess()
        self._metadata.names.get(self.find(obj))
    
    def _getIDByName(self,name):
        for id,n in self._metadata.names.iteritems():
            if n == name:
                return id
        return -1
        
    def _remove(self, obj, auto_removed=0):
        id = self.find(obj)
        try:
            del self._metadata.names[id]
        except KeyError:
            pass
        super(BoxRegistry,self)._remove(obj,auto_removed)

    def getIndexCache(self,obj):
        if obj == self._metadata:
            return {}
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
        obj = _unwrap(obj)
        obj = obj.clone()
        self._add(obj)
        self._setName(obj,name)

    def proxy_rename(self,obj_id,name):
        self._setName(self._get_obj(obj_id), name)
    
    def proxy_remove(self,obj_id):
        self._remove(self._get_obj(obj_id))

    def getProxy(self):
        slice = BoxRegistrySlice(self.name)
        slice.objects = self
        proxy = BoxRegistrySliceProxy(slice)
        proxy.add = self.proxy_add
        proxy.rename = self.proxy_rename
        proxy.remove = self.proxy_remove
        return proxy



from RegistrySlice import RegistrySlice 
from Ganga.Core.GangaRepository import getRegistry
class BoxRegistrySlice(RegistrySlice):
    def __init__(self,name):
        super(BoxRegistrySlice,self).__init__(name,display_prefix="box")
        self._display_columns_functions["id"] = lambda obj : obj._getRegistryID()
        self._display_columns_functions["type"] = lambda obj : obj._name
        self._display_columns_functions["name"] = lambda obj : obj._getRegistry()._metadata.names.get(obj._getRegistryID(),"")
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



from RegistrySliceProxy import RegistrySliceProxy, _wrap, _unwrap
class BoxRegistrySliceProxy(RegistrySliceProxy):
    """This object is a list of objects in the box
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
        items = self._impl.objects.items()
        for id,obj in items:
            reg = obj._getRegistry()
            if not reg is None:
                reg._remove(obj)
