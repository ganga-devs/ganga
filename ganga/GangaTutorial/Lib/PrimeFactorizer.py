################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: PrimeFactorizer.py,v 1.2 2009-04-23 13:47:59 moscicki Exp $
################################################################################

import os
import shutil
from GangaCore.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaCore.GPIDev.Adapters.IPrepareApp import IPrepareApp
from GangaCore.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from GangaCore.GPIDev.Schema import *
from GangaCore.Utility.Config import getConfig
from GangaCore.GPIDev.Lib.File import File, ShareDir
from GangaCore.Utility.logging import getLogger
from GangaCore.GPIDev.Base.Proxy import getName

logger = getLogger()

class PrimeFactorizer(IPrepareApp):
    """
    PrimeFactorizer application -- factorize any arbitrary number into prime numbers.
    """

    _schema = Schema(Version(1, 1), {
        'number': SimpleItem(defvalue=1, sequence=0, typelist=['int', 'long'], doc='The number to be factorized.'),
        'is_prepared': SimpleItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, hidden=0, typelist=[None, ShareDir], protected=0, comparable=1, doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
        'hash': SimpleItem(defvalue=None, typelist=[None, str], hidden=0, doc='MD5 hash of the string representation of applications preparable attributes'),
    })

    _category = 'applications'
    _name = 'PrimeFactorizer'
    _exportmethods = ['prepare', 'unprepare']
    # _GUIPrefs = [ { 'attribute' : 'number', 'widget' : 'Integer' },
    #              { 'attribute' : 'prime_table', 'widget' : 'String' }]

    def __init__(self):
        super(PrimeFactorizer, self).__init__()

    def configure(self, masterappconfig):
        from GangaCore.GPI import TUTDIR
        from GangaCore.Core.exceptions import ApplicationConfigurationError
        import os.path

        # the prime number factorizer executable
        self.exe = File(TUTDIR + '/Lib/primes/prime_factor.py')

        prime_tables = []
        try:
            job = self.getJobObject()
            prime_tables = job.inputdata.get_dataset()
        except:
            pass

        # the arguments of the executable
        # - arg1: the number
        # - arg2: the SURL of the prime number lookup table
        self.args = ['%s' % self.number] + prime_tables
        self.inputs = []
        self.outputs = ['*.dat']
        self.envs = {}
        return (None, None)

    def unprepare(self, force=False):
        """
        Revert an Executable() application back to it's unprepared state.
        """
        logger.debug('Running unprepare in Executable app')
        if self.is_prepared is not None:
            self.decrementShareCounter(self.is_prepared)
            self.is_prepared = None
        self.hash = None

    def prepare(self, force=False):
        from GangaCore.Core.exceptions import ApplicationPrepareError

        if (self.is_prepared is not None) and not force:
            raise ApplicationPrepareError(
                '%s application has already been prepared. Use prepare(force=True) to prepare again.' % getName(self))

        # lets use the same criteria as the configure() method for checking file existence & sanity
        # this will bail us out of prepare if there's somthing odd with the job config - like the executable
        # file is unspecified, has a space or is a relative path
        self.configure(self)
        logger.info('Preparing %s application.' % getName(self))
        self.is_prepared = ShareDir()
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

            if isinstance(self.exe, str):
                source = self.exe
            elif isinstance(self.exe, File):
                source = self.exe.name

            if isinstance(self.exe, str):
                logger.debug("exe is a string so no copying")
            elif not os.path.exists(source):
                logger.debug("Error copying exe: %s to input workspace" %
                            str(source))
            else:
                try:
                    parent_job = self.getJobObject()
                except:
                    parent_job = None
                    pass
                if parent_job is not None:
                    input_dir = parent_job.getInputWorkspace(create=True).getPath()
                    shutil.copy2(source, input_dir)

        except Exception as err:
            logger.debug("Err: %s" % str(err))
            self.unprepare()
            raise

        return 1


# FIXME: a cleaner solution, which is integrated with type information in schemas should be used automatically
#config = getConfig('GangaTutorial_Properties')
mc = getConfig('MonitoringServices')
mc.addOption('GangaTutorial', None, '')


class RTHandler(IRuntimeHandler):
    def prepare(self, app, appconfig, appmasterconfig, jobmasterconfig):
        from GangaCore.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

        c = StandardJobConfig(app.exe, app.inputs, app.args, app.outputs, app.envs)
       
        return c


class LCGRTHandler(IRuntimeHandler):
    def prepare(self, app, appconfig, appmasterconfig, jobmasterconfig):
        from GangaCore.Lib.LCG import LCGJobConfig

        c = LCGJobConfig(app.exe, app.inputs, app.args, app.outputs, app.envs)
        c.monitoring_svc = mc['GangaTutorial']
        
        return c

allHandlers.add('PrimeFactorizer', 'LSF', RTHandler)
allHandlers.add('PrimeFactorizer', 'Local', RTHandler)
allHandlers.add('PrimeFactorizer', 'PBS', RTHandler)
allHandlers.add('PrimeFactorizer', 'SGE', RTHandler)
allHandlers.add('PrimeFactorizer', 'Condor', RTHandler)
allHandlers.add('PrimeFactorizer', 'LCG', LCGRTHandler)
allHandlers.add('PrimeFactorizer', 'TestSubmitter', RTHandler)
allHandlers.add('PrimeFactorizer', 'Interactive', RTHandler)
allHandlers.add('PrimeFactorizer', 'Batch', RTHandler)

##############################################################
# handler for NG

class NGRTHandler(IRuntimeHandler):

    def prepare(self, app, appconfig, appmasterconfig, jobmasterconfig):
        from GangaNG.Lib.NG import NGJobConfig
        return NGJobConfig(app.exe, app.inputs, app.args, app.outputs, app.envs)
