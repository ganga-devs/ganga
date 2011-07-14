import os
import types
import atexit
import shutil
from Ganga.Core.GangaRepository.Registry import Registry, RegistryKeyError, RegistryAccessError
from Ganga.GPIDev.Base   import GangaObject
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from Ganga.GPIDev.Schema import Schema, SimpleItem, Version
from Ganga.GPIDev.Lib.Registry.JobRegistry import RegistryAccessError, RegistryKeyError
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy, _wrap
from Ganga.GPIDev.Base.Proxy import GPIProxyObject
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()


class PrepRegistry(Registry):

    def startup(self):
        self._needs_metadata = True
        super(PrepRegistry,self).startup()
        if len(self.metadata.ids()) == 0:
            self.metadata._add(ShareRef())            
        self.shareref = self.metadata[self.metadata.ids()[-1]]

    def getShareRef(self):
        return self.shareref


    def getProxy(self):
        pass
    
    def shutdown(self):
        self.shareref = self.metadata[self.metadata.ids()[-1]]
        self.shareref.closedown()





class ShareRef(GangaObject):
    """The shareref (shared files reference) object in Ganga gives 
    you a mechanism for reusing "prepared" applications with multiple jobs.
    shareref persists between Ganga sessions.
    """
    _schema = Schema(Version(1,2),{ 'name':SimpleItem({}, protected=1,copyable=1,hidden=1)})

    _category = 'sharerefs'
    _name = 'ShareRef'
    _exportmethods = ['add','remove','printtree']

    default_registry = 'prep'
   
    def __init__(self):
        super(ShareRef, self).__init__()
        self._setRegistry(None)

    def __getstate__(self):
        dict = super(ShareRef, self).__getstate__()
        dict['_registry'] = None
        dict['_counter']    = 0
        return dict

    def __setstate__(self, dict):
        self._getWriteAccess()
        try:
            super(ShareRef, self).__setstate__(dict)
            self._setRegistry(None)
            self._setDirty()
        finally:
            self._releaseWriteAccess()
        
    def add(self,shareddir):
        """Adds shared object to the ShareRef 'tree', incrementing a reference counter
        as appropriate.
        """
        self._getWriteAccess()
        
        if shareddir not in self.name:
            logger.info('%s is not stored in the shareref metadata object...adding.' %shareddir)
            self.name[shareddir] = 1
        else:
            self.name[shareddir] += 1

        self._setDirty()
        self._releaseWriteAccess()


    def remove(self,shareddir):
        """Reduces the metadata reference counter for a given shared object by 1. If the current value
        of the counter is 0, the shared object will be removed from the metadata, and the files deleted
        when Ganga exits.
        """
        self._getWriteAccess()
        
        if shareddir not in self.name:
            logger.info('%s is not stored in the shareref metadata object.' %shareddir)
            self.name[shareddir] = 1
        elif self.name[shareddir] == 1:
            #self.name.pop(shareddir) 
            self.name[shareddir] -= 1
        elif self.name[shareddir] == 0:
            pass
        else:
            self.name[shareddir] -= 1

        self._setDirty()
        self._releaseWriteAccess()


    def closedown(self):
        """Cleans up the ShareDir upon shutdown of the registry, ie. when exiting a Ganga session."""
        self._getWriteAccess()
        for shareddir in self.name:
            #logger.info('Checking %s' %shareddir)
            if self.name[shareddir] == 0:
                logger.info('%s no longer being referenced by any Prepared() jobs...removing directory.' %shareddir)
                if os.path.isdir(shareddir):
                    shutil.rmtree(shareddir)
                #shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
                #shareref.remove(shareddir)


        #should also check that any directories contained within the sharetree exist, otherwise remove their references.


        self._setDirty()
        self._releaseWriteAccess()

    def _display(self):
        """Prints content of the shareref metadata in a well formatted way.
        """

        return self.name

    def _proxy_display(self,stuff):
        return self._display()


class _proxy_display(object):
    def __get__(self, obj, cls):
        if obj is None:
            return cls._impl._proxy_display
        return obj._impl._proxy_display

      

#class _copy(object):
#    def __get__(self, obj, cls):
#        if obj is None:
#            return cls._impl._copy
#        return obj._impl._copy   

ShareRef.__str__              = ShareRef._display    
ShareRef._proxyClass._display = _proxy_display()
ShareRef._proxyClass.__str__  = _proxy_display()
#ShareRef._proxyClass.copy = _copy()
