##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: IApplication.py,v 1.1 2008-07-17 16:40:52 moscicki Exp $
##########################################################################

from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.Core.GangaRepository import getRegistry
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory, isType
from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
from Ganga.GPIDev.Lib.File import File
import os
import shutil
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()


from Ganga.Utility.Config import getConfig
from Ganga.Utility.files import expandfilename

config = getConfig("Preparable")

class IPrepareApp(IApplication):

    """
    Base class for all applications which can be placed into a prepared\
    state. 
    """
    _schema = Schema(Version(0, 0), {'hash': SimpleItem(defvalue=None, typelist=[None, str], hidden=1)})
    _category = 'applications'
    _name = 'PrepareApp'
    _hidden = 1

    def __init__(self):
        super(IPrepareApp, self).__init__()

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

    def post_prepare(self):
        """
        Put any methods that should always be run at the end of the preparation process here.
        """
        self.calc_hash()

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
        self.hash = None

    def copyPreparables(self):
        """
        This method iterates over all attributes in an application and decides\
        whether they should be persisted (i.e. copied) in the Shared Directory\
        when the application is prepared.
        If an IOError is raised when attempting the copy, this method returns 0\
        otherwise it returns 1.
        """
        send_to_sharedir = []
        for name, item in self._schema.allItems():
            if item['preparable']:
                logger.debug('Found preparable %s' % (name))
                logger.debug('adding to sharedir %s' % (self.__getattribute__(name)))
                send_to_sharedir.append(self.__getattribute__(name))
        shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']), 'shared', getConfig('Configuration')['user'])
        for prepitem in send_to_sharedir:
            logger.debug('working on %s' % (prepitem))
            # we may have a list of files/strings
            if isType(prepitem, (list, GangaList)):
                logger.debug('found a list')
                for subitem in prepitem:
                    if isType(subitem, str):
                        # we have a file. if it's an absolute path, copy it to
                        # the shared dir
                        if os.path.abspath(subitem) == subitem:
                            logger.debug('Sending file %s to shared directory.' % (subitem))
                            try:
                                shr_dir = os.path.join(shared_path, self.is_prepared.name)
                                if not os.path.isidr(shr_dir):
                                    os.makedirs(shr_dir)
                                shutil.copy2(subitem, shr_dir)
                                logger.debug("Copying into: %s" % shr_dir)
                            except IOError as e:
                                logger.error(e)
                                return 0
                    elif isType(subitem, File) and subitem.name is not '':
                        logger.debug('Sending file object %s to shared directory' % subitem.name)
                        try:
                            shr_dir = os.path.join(shared_path, self.is_prepared.name)
                            if not os.path.isdir(shr_dir):
                                os.makedirs(shr_dir)
                                logger.debug("Copying into: %s" % shr_dir)
                            shutil.copy2(subitem.name, shr_dir)
                        except IOError as e:
                            logger.error(e)
                            return 0
            elif isinstance(prepitem, str):
                logger.debug('found a string')
                # we have a file. if it's an absolute path, copy it to the
                # shared dir
                if os.path.abspath(prepitem) == prepitem:
                    logger.debug('Sending string file %s to shared directory.' % (prepitem))
                    try:
                        shr_dir = os.path.join(shared_path, self.is_prepared.name)
                        if not os.path.isdir(shr_dir):
                            os.makedirs(shr_dir)
                        logger.debug("Copying into: %s" % shr_dir)
                        shutil.copy2(prepitem, shr_dir)
                    except IOError as e:
                        logger.error(e)
                        return 0
            elif isType(prepitem, File) and prepitem.name is not '':
                logger.debug('found a file')
                logger.debug('Sending "File" object %s to shared directory' % prepitem.name)
                try:
                    shr_dir = os.path.join(shared_path, self.is_prepared.name)
                    if not os.path.isdir(shr_dir):
                        os.makedirs(shr_dir)
                    logger.debug("Copying into: %s" % shr_dir)
                    shutil.copy2(prepitem.name, shr_dir)
                except IOError as e:
                    logger.error(e)
                    return 0
            else:
                logger.debug('Nothing worth copying found in %s' % (prepitem))
        return 1

    def calc_hash(self, verify=False):
        """Calculate the MD5 digest of the application's preparable attribute(s), and store
        that value in the application schema. The value is recalculated (and compared against
        the initial value) every time the application is written to the Ganga repository. This
        allows warnings to be generated should an application's locked attributes be changed 
        post-preparation.
        """
        from Ganga.GPIDev.Base.Proxy import runProxyMethod
        import cStringIO
        try:
            import hashlib
            digest = hashlib.new('md5')
        except Exception as err:
            logger.debug("Err: %s" % err)
            import md5
            digest = md5.new()

        sio = cStringIO.StringIO()
        runProxyMethod(self, 'printPrepTree', sio)
        digest.update(str(sio.getvalue()))
        tmp = sio.getvalue()
        if verify == False:
            self.hash = digest.hexdigest()
        else:
            # we return true if this is called with verify=True and the current hash is the same as that stored in the schema.
            # this is checked immediately prior to (re)writing the object to
            # the repository
            return digest.hexdigest() == self.hash

    def incrementShareCounter(self, shared_directory_name):
        logger.debug('Incrementing shared directory reference counter')
        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
        logger.debug('within incrementShareCounter, calling increase')
        shareref.increase(shared_directory_name)

    def decrementShareCounter(self, shared_directory_name, remove=0):
        remove = remove
        logger.debug('Decrementing shared directory reference counter')
        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
        shareref.decrease(shared_directory_name, remove)

    def listShareDirs(self):
        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
        shareref

    def listShareDirContents(self, shared_directory_name):
        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
        shareref.ls(shared_directory_name)

    def checkPreparedHasParent(self, prepared_object):
        if prepared_object._getRegistry() is None:
            self.incrementShareCounter(prepared_object.is_prepared.name)
            self.decrementShareCounter(prepared_object.is_prepared.name)
            logger.info('Application is not currently associated with a persisted Ganga object')
            logger.info('(e.g. box, job, task). Both the prepared application and the contents of')
            logger.info('its shared directory will be lost when Ganga exits.')
            logger.info('Shared directory location: %s' % (self.is_prepared.name))
            # logger.error(self.listShareDirContents(prepared_object.is_prepared.name))
        else:
            self.incrementShareCounter(prepared_object.is_prepared.name)

