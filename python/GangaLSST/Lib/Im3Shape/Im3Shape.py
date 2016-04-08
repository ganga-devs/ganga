##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Im3Shape.py,v 1.1 2008-07-17 16:40:57 moscicki Exp $
##########################################################################

from Ganga.GPIDev.Adapters.IGangaFile import IGangaFile
from Ganga.GPIDev.Adapters.IPrepareApp import IPrepareApp
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem

from Ganga.Utility.Config import getConfig

from Ganga.GPIDev.Lib.File import File, ShareDir
from Ganga.Core import ApplicationConfigurationError, ApplicationPrepareError

from Ganga.Utility.logging import getLogger

from Ganga.GPIDev.Base.Proxy import getName, isType, stripProxy

import os
import shutil
from Ganga.Utility.files import expandfilename

from GangaDirac.Lib.Files import DiracFile
from Ganga.GPIDev.Lib.File import LocalFile

logger = getLogger()

class Im3Shape(IPrepareApp):

    """

    """
    _schema = Schema(Version(1, 0), {
        'location': SimpleItem(defvalue=DiracFile(lfn='/lhcb/user/r/rcurrie/firstTestDiracFile.txt'),types=[IGangaFile], doc="Location of the Im3Shape program tarball"),
        'ini_location': SimpleItem(defvalue=LocalFile('myIniFile.ini'), types=[IGangaFile], doc=".ini file used to configure Im3Shape"),
        'env': SimpleItem(defvalue={}, typelist=['str'], doc='Dictionary of environment variables that will be replaced in the running environment.'),
        'is_prepared': SimpleItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, hidden=0, typelist=[None, 'bool', ShareDir], protected=0, comparable=1, doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
        'hash': SimpleItem(defvalue=None, typelist=['type(None)', 'str'], hidden=0, doc='MD5 hash of the string representation of applications preparable attributes'),
        'blacklist': SimpleItem(defvalue=DiracFile('/lhcb/user/r/rcurrie/secondTestDiracFile.txt'), types=[IGangaFile], doc="Blacklist file for running Im3Shape"),
        'rank': SimpleItem(defvalue=1, doc="Rank in the split of the tile from splitting"),
        'size': SimpleItem(defvalue=5, doc="Size of the splitting of the tile from splitting"),
        'catalog': SimpleItem(defvalue=None, types=[IGangaFile, None], doc="Catalog which is used to describe what is processed"),
    })
    _category = 'applications'
    _name = 'Im3Shape'
    _exportmethods = ['prepare', 'unprepare']

    def __init__(self):
        super(Im3Shape, self).__init__()

    def __deepcopy__(self, memo):
        return super(Im3Shape, self).__deepcopy__(memo)

    def unprepare(self, force=False):
        """
        Revert an Im3Shape() application back to it's unprepared state.
        """
        logger.debug('Running unprepare in Im3Shape app')
        if self.is_prepared is not None:
            self.decrementShareCounter(self.is_prepared.name)
            self.is_prepared = None
        self.hash = None

    def prepare(self, force=False):
        """
        A method to place the Im3Shape application into a prepared state.

        The application wil have a Shared Directory object created for it. 
        If the application's 'exe' attribute references a File() object or
        is a string equivalent to the absolute path of a file, the file 
        will be copied into the Shared Directory.

        Otherwise, it is assumed that the 'exe' attribute is referencing a 
        file available in the user's path (as per the default "echo Hello World"
        example). In this case, a wrapper script which calls this same command 
        is created and placed into the Shared Directory.

        When the application is submitted for execution, it is the contents of the
        Shared Directory that are shipped to the execution backend. 

        The Shared Directory contents can be queried with 
        shareref.ls('directory_name')

        See help(shareref) for further information.
        """

        if (self.is_prepared is not None) and (force is not True):
            raise ApplicationPrepareError('%s application has already been prepared. Use prepare(force=True) to prepare again.' % getName(self))

        # lets use the same criteria as the configure() method for checking file existence & sanity
        # this will bail us out of prepare if there's somthing odd with the job config - like the executable
        # file is unspecified, has a space or is a relative path
        self.configure(self)
        logger.info('Preparing %s application.' % getName(self))
        setattr(self, 'is_prepared', ShareDir())
        logger.info('Created shared directory: %s' % (self.is_prepared.name))

        try:
            # copy any 'preparable' objects into the shared directory
            send_to_sharedir = self.copyPreparables()
            # add the newly created shared directory into the metadata system
            # if the app is associated with a persisted object
            self.checkPreparedHasParent(self)
            # return
            # [os.path.join(self.is_prepared.name,os.path.basename(send_to_sharedir))]
            self.post_prepare()

        except Exception as err:
            logger.debug("Err: %s" % str(err))
            self.unprepare()
            raise err

        return 1

    def configure(self, masterappconfig):

        return (None, None)

