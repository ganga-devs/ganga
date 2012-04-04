################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: IApplication.py,v 1.1 2008-07-17 16:40:52 moscicki Exp $
################################################################################

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.Core.GangaRepository import getRegistry
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory, isType
from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
from Ganga.GPIDev.Lib.File import *
import os, shutil
from Ganga.GPIDev.Schema import *

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()


from Ganga.Utility.Config import makeConfig, ConfigError, getConfig
from Ganga.Utility.files import expandfilename
config = makeConfig('Preparable', 'Parameters for preparable applications')
config.addOption('unprepare_on_copy', False, 'Unprepare a prepared application when it is copied')
shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),'shared',getConfig('Configuration')['user'])

class IPrepareApp(IApplication):
    """
    Base class for all applications which can be placed into a prepared\
    state. 
    """
    _schema =  Schema(Version(0,0), {})
    _category='applications'
    _name = 'PrepareApp'
    _hidden = 1

#    def _readonly(self):
#        """An application is read-only once it has been prepared."""
#        if self.is_prepared is None:
#            return 0
#        else:
#            logger.error("Cannot modify a prepared application's attributes. First unprepare() the application.")
#            return 1


    def _auto__init__(self, unprepare=None):
        if unprepare is True:
            logger.debug("Calling unprepare() from IPrepareApp's _auto__init__()")
            self.unprepare()


    def prepare(self, force=False):
        """
        Base class for all applications which can be placed into a prepared\
        state. 

        """
        pass


    def unprepare(self, force=False):
        """
        Revert an application back to the exact state it was in prior to being\
        prepared.
        """
        logger.debug("Running unprepare() from IPrepareApp")
        if self.is_prepared is True:
            self.is_prepared = None
        elif self.is_prepared is not None:
            self.is_prepared = None


    def copyPreparables(self):
        """
        This method iterates over all attributes in an application and decides\
        whether they should be persisted (i.e. copied) in the Shared Directory\
        when the application is prepared.
        """
        send_to_sharedir = []
        for name,item in self._schema.allItems():
            if item['preparable']:
                logger.debug('Found preparable %s' %(name))
                logger.debug('adding to sharedir %s' %(self.__getattribute__(name)))
                send_to_sharedir.append(self.__getattribute__(name))

    
        for prepitem in send_to_sharedir:
            logger.debug('working on %s' %(prepitem))
            #we may have a list of files/strings
            if type(prepitem) is list or type(prepitem) is GangaList:
                logger.debug('found a list')
                for subitem in prepitem:
                    if isType(subitem, str):
                    #we have a file. if it's an absolute path, copy it to the shared dir
                        if os.path.abspath(subitem) == subitem:
                            logger.info('Sending file %s to shared directory.'%(subitem))
                            shutil.copy2(subitem, os.path.join(shared_path,self.is_prepared.name))
                            #else assume it's a system binary (or other attribute), so we don't need to transport anything to the sharedir
                        else:
                            pass
                            #logger.debug('\'%s\', assumed to be available in $PATH'%(subitem))
                    elif type(subitem) is File and subitem.name is not '':
                        logger.info('Sending file object %s to shared directory'%subitem.name)
                        shutil.copy2(subitem.name, os.path.join(shared_path,self.is_prepared.name))
            elif type(prepitem) is str:
                logger.debug('found a string')
                #we have a file. if it's an absolute path, copy it to the shared dir
                if os.path.abspath(prepitem) == prepitem:
                    logger.info('Sending file %s to shared directory.'%(prepitem))
                    shutil.copy2(prepitem, os.path.join(shared_path,self.is_prepared.name))
                    #else assume it's a system binary (or other attribute), so we don't need to transport anything to the sharedir
                else:
                    pass
                    #logger.info('Preparing application to use \'%s\', assumed to be available in $PATH'%(prepitem))
            elif type(prepitem) is File and prepitem.name is not '':
                logger.debug('found a file')
                logger.info('Sending file object %s to shared directory'%prepitem.name)
                shutil.copy2(prepitem.name, os.path.join(shared_path,self.is_prepared.name))
            else:
                logger.debug('Nothing worth copying found in %s' %(prepitem))
        return 


    def incrementShareCounter(self, shared_directory_name):
        logger.debug('Incrementing shared directory reference counter')
        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
        logger.debug('within incrementShareCounter, calling increase')
        shareref.increase(shared_directory_name)


    def decrementShareCounter(self, shared_directory_name):
        logger.debug('Decrementing shared directory reference counter')
        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
        shareref.decrease(shared_directory_name)

    def listShareDirs(self):
        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
        shareref

    def listShareDirContents(self,shared_directory_name):
        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
        shareref.ls(shared_directory_name)

    def checkPreparedHasParent(self, prepared_object):
        if prepared_object._getRegistry() is None:
            self.incrementShareCounter(prepared_object.is_prepared.name)
            self.decrementShareCounter(prepared_object.is_prepared.name)
            logger.warn('Application is not currently associated with a persisted Ganga object')
            logger.warn('(e.g. box, job, task). Both the prepared application and the contents of')
            logger.warn('its shared directory will be lost when Ganga exits.')
            logger.warn('Shared directory location: %s' %(self.is_prepared.name))
            logger.warn('Shared directory contents:')
            self.listShareDirContents(prepared_object.is_prepared.name)
        else:
            self.incrementShareCounter(prepared_object.is_prepared.name)




