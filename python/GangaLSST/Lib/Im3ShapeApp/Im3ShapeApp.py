##########################################################################
# Ganga Project. http://cern.ch/ganga
##########################################################################

from Ganga.GPIDev.Adapters.IGangaFile import IGangaFile
from Ganga.GPIDev.Adapters.IPrepareApp import IPrepareApp
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem, GangaFileItem

from Ganga.Utility.Config import getConfig

from Ganga.GPIDev.Lib.File import File, ShareDir
from Ganga.Core import ApplicationConfigurationError, ApplicationPrepareError

from Ganga.Utility.logging import getLogger

from Ganga.GPIDev.Base.Proxy import getName, isType, stripProxy

import os
import shutil
from Ganga.Utility.files import expandfilename

from GangaDirac.Lib.Files.DiracFile import DiracFile
from Ganga.GPIDev.Lib.File import LocalFile

logger = getLogger()

class Im3ShapeApp(IPrepareApp):

    """
    This App is to store the configuration of the Im3Shape app which is to be run according to a given set of configs:

    i.e.
    ./run_dir/exe_name <someData> ini_location catalog <someOutput> rank size

    e.g.
    ./run_dir/run-im3shape someData.fz ini_file.ini all someData.fz.0.20 0 20

    The input and output file names are configured in the RTHandler based upon the inputdata given to a particular job.

    The im3_location is the path to a .tgz file (or some file which can be extracted by the RTHandler) which gives the im3shape-grid (run_dir) folder containing the run-im3shape app (exe-name) on the WN
    """
    _schema = Schema(Version(1, 0), {
        ## Required to configure Im3ShapeApp
        'im3_location': GangaFileItem(defvalue=None, doc="Location of the Im3Shape program tarball"),
        'exe_name': SimpleItem(defvalue='run-im3shape', doc="Name of the im3shape binary"),
        'ini_location': GangaFileItem(defvalue=None, doc=".ini file used to configure Im3Shape"),
        'blacklist': GangaFileItem(defvalue=None, doc="Blacklist file for running Im3Shape"),
        'rank': SimpleItem(defvalue=0, doc="Rank in the split of the tile from splitting"),
        'size': SimpleItem(defvalue=200, doc="Size of the splitting of the tile from splitting"),
        'catalog': SimpleItem(defvalue='all', types=[str], doc="Catalog which is used to describe what is processed"),
        'run_dir': SimpleItem(defvalue='im3shape-grid', types=[str], doc="Directory on the WN where the binary is"),
        ## Below is needed for prepared state stuff
        'is_prepared': SimpleItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, hidden=0, typelist=[None, ShareDir], protected=0, comparable=1, doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
        'hash': SimpleItem(defvalue=None, typelist=[None, str], hidden=0, doc='MD5 hash of the string representation of applications preparable attributes'),
    })
    _category = 'applications'
    _name = 'Im3ShapeApp'
    _exportmethods = ['prepare', 'unprepare']

    def unprepare(self, force=False):
        """
        Revert an Im3ShapeApp application back to it's unprepared state.
        args:
            force (bool): should force the unprepare step to run
        """
        logger.debug('Running unprepare in Im3ShapeApp')
        if self.is_prepared is not None:
            self.decrementShareCounter(self.is_prepared.name)
            self.is_prepared = None
        self.hash = None

    def prepare(self, force=False):
        """
        This prepares the Im3ShapeApp application and copies any LocalFile objects which are allocated to: im3_location, ini_location and blacklist into the prepared sandbox to be shipped to the WN
        Args:
            force (bool): Should force the prepare step to run
        """

        if (self.is_prepared is not None) and (force is not True):
            raise ApplicationPrepareError('%s application has already been prepared. Use prepare(force=True) to prepare again.' % getName(self))

        logger.info('Preparing %s application.' % getName(self))
        self.is_prepared = ShareDir()
        logger.info('Created shared directory: %s' % (self.is_prepared.name))

        try:
            # copy any 'preparable' objects into the shared directory
            send_to_sharedir = self.copyPreparables()
            # add the newly created shared directory into the metadata system
            # if the app is associated with a persisted object
            self.checkPreparedHasParent(self)
            for file_ in [self.ini_location, self.im3_location, self.blacklist]:
                if isinstance(file_, LocalFile):
                    self.copyIntoPrepDir(file_.namePattern)
                assert type(file_) in [LocalFile, DiracFile]
            # return
            # [os.path.join(self.is_prepared.name,os.path.basename(send_to_sharedir))]
            self.post_prepare()

        except Exception as err:
            logger.debug("Err: %s" % str(err))
            self.unprepare()
            raise

        return 1

    def configure(self, masterappconfig):
        """
        This is a null-op effecitvely, we may add something here in the future but this function is stub
        This is required so that the job will submit
        """
        return (None, None)

