##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: IApplication.py,v 1.1 2008-07-17 16:40:52 moscicki Exp $
##########################################################################

import os
import shutil

from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Schema.Schema import Schema, Version, SimpleItem

from Ganga.Utility.Config.Config import makeConfig
config = makeConfig('Preparable', 'Parameters for preparable applications')
config.addOption('unprepare_on_copy', False, 'Unprepare a prepared application when it is copied')


class IPrepareApp(IApplication):

    """
    Base class for all applications which can be placed into a prepared\
    state. 
    """
    _schema = Schema(Version(0, 0), {'hash': SimpleItem(
        defvalue=None, typelist=['type(None)', 'str'], hidden=1)})
    _category = 'applications'
    _name = 'PrepareApp'
    _hidden = 1

#    def _readonly(self):
#        """An application is read-only once it has been prepared."""
#        if self.is_prepared is None:
#            return 0
#        else:
#            logging.getLogger(__name__).error("Cannot modify a prepared application's attributes. First unprepare() the application.")
#            return 1

    def _auto__init__(self, unprepare=None):
        if unprepare is True:
            logging.getLogger(__name__).debug(
                "Calling unprepare() from IPrepareApp's _auto__init__()")
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
        logging.getLogger(__name__).debug("Running unprepare() from IPrepareApp")
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
        from Ganga.GPIDev.Base.Proxy import isType
        from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
        from Ganga.GPIDev.Lib.File import File
        from Ganga.Utility.Config import getConfig
        from Ganga.Utility.files import expandfilename

        send_to_sharedir = []
        for name, item in self._schema.allItems():
            if item['preparable']:
                logging.getLogger(__name__).debug('Found preparable %s' % (name))
                logging.getLogger(__name__).debug('adding to sharedir %s' %
                             (self.__getattribute__(name)))
                send_to_sharedir.append(self.__getattribute__(name))
        shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                                   'shared', getConfig('Configuration')['user'])
        for prepitem in send_to_sharedir:
            logging.getLogger(__name__).debug('working on %s' % (prepitem))
            # we may have a list of files/strings
            if isinstance(prepitem, (list, GangaList)):
                logging.getLogger(__name__).debug('found a list')
                for subitem in prepitem:
                    if isType(subitem, str):
                        # we have a file. if it's an absolute path, copy it to
                        # the shared dir
                        if os.path.abspath(subitem) == subitem:
                            logging.getLogger(__name__).info(
                                'Sending file %s to shared directory.' % (subitem))
                            try:
                                shr_dir = os.path.join(
                                    shared_path, self.is_prepared.name)
                                if not os.path.isidr(shr_dir):
                                    os.makedirs(shr_dir)
                                shutil.copy2(subitem, shr_dir)
                            except IOError as e:
                                logging.getLogger(__name__).error(e)
                                return 0
                    elif isinstance(subitem, File) and subitem.name is not '':
                        logging.getLogger(__name__).info(
                            'Sending file object %s to shared directory' % subitem.name)
                        try:
                            shr_dir = os.path.join(
                                shared_path, self.is_prepared.name)
                            if not os.path.isdir(shr_dir):
                                os.makedirs(shr_dir)
                            shutil.copy2(subitem.name, shr_dir)
                        except IOError as e:
                            logging.getLogger(__name__).error(e)
                            return 0
            elif isinstance(prepitem, str):
                logging.getLogger(__name__).debug('found a string')
                # we have a file. if it's an absolute path, copy it to the
                # shared dir
                if os.path.abspath(prepitem) == prepitem:
                    logging.getLogger(__name__).info(
                        'Sending file %s to shared directory.' % (prepitem))
                    try:
                        shr_dir = os.path.join(
                            shared_path, self.is_prepared.name)
                        if not os.path.isdir(shr_dir):
                            os.makedirs(shr_dir)
                        shutil.copy2(prepitem, shr_dir)
                    except IOError as e:
                        logging.getLogger(__name__).error(e)
                        return 0
            elif isinstance(prepitem, File) and prepitem.name is not '':
                logging.getLogger(__name__).debug('found a file')
                logging.getLogger(__name__).info(
                    'Sending file object %s to shared directory' % prepitem.name)
                try:
                    shr_dir = os.path.join(shared_path, self.is_prepared.name)
                    if not os.path.isdir(shr_dir):
                        os.makedirs(shr_dir)
                    shutil.copy2(prepitem.name, shr_dir)
                except IOError as e:
                    logging.getLogger(__name__).error(e)
                    return 0
            else:
                logging.getLogger(__name__).debug('Nothing worth copying found in %s' % (prepitem))
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
        except:
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
        from Ganga.Core.GangaRepository import getRegistry
        from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory

        logging.getLogger(__name__).debug('Incrementing shared directory reference counter')
        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
        logging.getLogger(__name__).debug('within incrementShareCounter, calling increase')
        shareref.increase(shared_directory_name)

    def decrementShareCounter(self, shared_directory_name, remove=0):
        from Ganga.Core.GangaRepository import getRegistry
        from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory

        remove = remove
        logging.getLogger(__name__).debug('Decrementing shared directory reference counter')
        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
        shareref.decrease(shared_directory_name, remove)

    def listShareDirs(self):
        from Ganga.Core.GangaRepository import getRegistry
        from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory

        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
        shareref

    def listShareDirContents(self, shared_directory_name):
        from Ganga.Core.GangaRepository import getRegistry
        from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory

        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
        shareref.ls(shared_directory_name)

    def checkPreparedHasParent(self, prepared_object):
        if prepared_object._getRegistry() is None:
            self.incrementShareCounter(prepared_object.is_prepared.name)
            self.decrementShareCounter(prepared_object.is_prepared.name)
            logging.getLogger(__name__).info(
                'Application is not currently associated with a persisted Ganga object')
            logging.getLogger(__name__).debug(
                '(e.g. box, job, task). Both the prepared application and the contents of')
            logging.getLogger(__name__).debug('its shared directory will be lost when Ganga exits.')
            logging.getLogger(__name__).debug('Shared directory location: %s' %
                         (self.is_prepared.name))
            # logging.getLogger(__name__).error(self.listShareDirContents(prepared_object.is_prepared.name))
        else:
            self.incrementShareCounter(prepared_object.is_prepared.name)
