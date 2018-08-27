##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: IApplication.py,v 1.1 2008-07-17 16:40:52 moscicki Exp $
##########################################################################

from GangaCore.GPIDev.Adapters.IApplication import IApplication
from GangaCore.Core.GangaRepository import getRegistry
from GangaCore.GPIDev.Base.Proxy import GPIProxyObjectFactory, isType
from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList
from GangaCore.GPIDev.Lib.File.File import File, ShareDir
import os
import shutil
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem

import GangaCore.Utility.logging
logger = GangaCore.Utility.logging.getLogger()


from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.files import expandfilename

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

    __slots__ = list()

    def _auto__init__(self, unprepare=None):
        """
        Function called when initializing from the Proxy layer i.e. interactive prompt or 'import ganga'
        Args:
            unprepare (bool): a parameter which unprepares an app when it's created new i.e. don't copy prepared sandboxes
        """
        if unprepare is True:
            logger.debug("Calling unprepare() from IPrepareApp's _auto__init__()")
            self.unprepare()

    def prepare(self, force=False):
        """
        Base class for all applications which can be placed into a prepared\
        state. 
        Args:
            force (bool) : forces the prepare function to be called no matter what when True
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
        Args:
            force (bool): causes unprepare to run always or not if True
        """
        logger.debug("Running unprepare() from IPrepareApp")
        if self.is_prepared is True:
            self.is_prepared = None
        elif self.is_prepared is not None:
            self.is_prepared = None
        self.hash = None

    def getSharedPath(self):
        """
        Return the full path of the shared directory where files are placed
        """
        share_folder = os.path.join(expandfilename(getConfig('Configuration')['gangadir']), 'shared', getConfig('Configuration')['user'])
        if isinstance(self.is_prepared, ShareDir):
            return os.path.join(share_folder, self.is_prepared.name)
        else:
            return share_folder

    def copyIntoPrepDir(self, obj2copy):
        """
        Method for actually copying the "obj2copy" object to the prepared state dir of this application
        Args:
            obj2copy (bool): is a string (local) address of a file to be copied as it's passed to shutil.copy2
        """
        shr_dir = self.getSharedPath()

        if not os.path.isdir(shr_dir):
            os.makedirs(shr_dir)
        shutil.copy2(obj2copy, shr_dir)
        logger.debug("Copying %s into: %s" % (obj2copy, shr_dir))

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
                                self.copyIntoPrepDir(subitem)
                            except IOError as e:
                                logger.error(e)
                                return 0
                    elif isType(subitem, File) and subitem.name is not '':
                        logger.debug('Sending file object %s to shared directory' % subitem.name)
                        try:
                            self.copyIntoPrepDir(subitem.name)
                        except IOError as e:
                            logger.error(e)
                            return 0
            elif isinstance(prepitem, str):
                logger.debug('found a string')
                # we have a file. if it's an absolute path, copy it to the
                # shared dir
                if prepitem and os.path.abspath(prepitem) == prepitem:
                    logger.debug('Sending string file %s to shared directory.' % (prepitem))
                    try:
                        self.copyIntoPrepDir(prepitem)
                    except IOError as e:
                        logger.error(e)
                        return 0
            elif isType(prepitem, File) and prepitem.name is not '':
                logger.debug('found a file')
                logger.debug('Sending "File" object %s to shared directory' % prepitem.name)
                try:
                    self.copyIntoPrepDir(prepitem.name)
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
        Args:
            verify (bool) : If the hash is to be verified in the future True save it to the hash schema attribute
        """
        from GangaCore.GPIDev.Base.Proxy import runProxyMethod
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

    #printPrepTree is only ever run on applications, from within IPrepareApp.py
    #if you (manually) try to run printPrepTree on anything other than an application, it will not work as expected
    #see the relevant code in VPrinter to understand why
    def printPrepTree(self, f=None, sel='preparable' ):
        ## After fixing some bugs we are left with incompatible job hashes. This should be addressd before removing
        ## This particular class!
        from GangaCore.GPIDev.Base.VPrinterOld import VPrinterOld
        self.accept(VPrinterOld(f, sel))

    def incrementShareCounter(self, shared_directory):
        """
        Function which is used to increment the number of (sub)jobs which share the prepared sandbox
        managed by this app
        Args:
            shared_directory (ShareDir): The ShareDir object managed by this app
        """
        logger.debug('Incrementing shared directory reference counter')
        shareref = getRegistry("prep").getShareRef()
        logger.debug('within incrementShareCounter, calling increase')
        shareref.increase(shared_directory)

    def decrementShareCounter(self, shared_directory, remove=0):
        """
        Function which is used to decrement the number of (sub)jobs which share the prepared sandbox
        managed by this app
        Args:
            shared_directory (ShareDir): The ShareDir object managed by this app
        """
        remove = remove
        logger.debug('Decrementing shared directory reference counter')
        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
        shareref.decrease(shared_directory, remove)

    def getShareCounterVal(self, shared_directory):
        """
        Function to get the current value of the counter
        Args:
            shared_directory_name (str): full name of directory managed by this app
        """
        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
        return shareref.counterVal(shared_directory)

    def listShareDirs(self):
        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
        shareref

    def listShareDirContents(self, shared_directory_name):
        """
        Function which is used to list the contents of the prepared sandbox folder managed by this app
        Args:
            shared_directory_name (str): full name of directory managed by this app
        """
        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
        shareref.ls(shared_directory_name)

    def checkPreparedHasParent(self, prepared_object):
        """
        Function which is used to check if a prepared app has a parent in a registry 
        Args:
            prepared_object (IPrepareApp): object in a registry which manages a prepared state
        """
        if prepared_object._getRegistry() is None:
            shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
            shareref.registerForRemoval(prepared_object.is_prepared.name)
            logger.info('Application is not currently associated with a persisted Ganga object')
            logger.info('(e.g. box, job, task). Both the prepared application and the contents of')
            logger.info('its shared directory will be lost when Ganga exits.')
            logger.info('Shared directory location: %s' % (self.is_prepared.name))
            # logger.error(self.listShareDirContents(prepared_object.is_prepared.name))
        else:
            self.incrementShareCounter(prepared_object.is_prepared)

