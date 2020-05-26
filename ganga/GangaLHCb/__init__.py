import json
import os
import os.path
import re
import datetime
from os.path import exists, isdir, realpath, isfile, islink
from os import pathsep, listdir, environ, fdopen
import subprocess
import GangaCore.Utility.logging
import GangaCore.Utility.Config
from optparse import OptionParser, OptionValueError

from GangaCore.Utility.Config.Config import _after_bootstrap
from GangaCore.Utility.logging import getLogger

from GangaCore.Runtime.GPIexport import exportToGPI
from GangaCore.Utility.execute import execute
from GangaCore.GPIDev.Credentials.CredentialStore import credential_store
from GangaDirac.Lib.Credentials.DiracProxy import DiracProxy
from GangaLHCb.Utility.LHCbDIRACenv import store_dirac_environment

logger = getLogger()

def guessPlatform():
    defaultPlatform = 'x86_64-centos7-gcc8-opt'
    cmd = '. /cvmfs/lhcb.cern.ch/lib/LbEnv &> /dev/null && python3 -c "import json, os; print(json.dumps(dict(os.environ.copy())))"'
    env = execute(cmd)
    if isinstance(env, str):
        try:
            env = json.loads(env)
        except Exception:
            logger.debug("Unable to extract platform - using default platform: %s" % defaultPlatform)
            return defaultPlatform
    if 'CMTCONFIG' in env.keys():
        defaultPlatform = env['CMTCONFIG']
        logger.debug("Setting the default application platform to %s" % defaultPlatform)
    else:
        logger.debug("Unable to extract platform - using default platform: %s" % defaultPlatform)

    return defaultPlatform


if not _after_bootstrap:
    configLHCb = GangaCore.Utility.Config.makeConfig('LHCb', 'Parameters for LHCb')

    # Set default values for the LHCb config section.
    dscrpt = 'The name of the local site to be used for resolving LFNs into PFNs.'
    configLHCb.addOption('LocalSite', '', dscrpt)

    dscrpt = 'Files from these services will go to the output sandbox (unless \
    overridden by the user in a specific job via the Job.outputdata field). Files \
    from all other known handlers will go to output data (unless overridden by \
    the user in a specific job via the Job.outputsandbox field).'
    configLHCb.addOption('outputsandbox_types',
                         ['CounterSummarySvc', 'NTupleSvc',
                          'HistogramPersistencySvc', 'MicroDSTStream',
                          'EvtTupleSvc'], dscrpt)
    dscrpt = 'The string that is added after the filename in the options to tell' \
             ' Gaudi how to read the data. This is the default value used if the '\
             'file name does not match any of the patterns in '\
             'datatype_string_patterns.'
    configLHCb.addOption('datatype_string_default',
                         """TYP='POOL_ROOTTREE' OPT='READ'""", dscrpt)
    dscrpt = 'If a file matches one of these patterns, then the string here '\
             'overrides the datatype_string_default value.'
    defval = {"SVC='LHCb::MDFSelector'": ['*.raw', '*.RAW', '*.mdf', '*.MDF']}
    configLHCb.addOption('datatype_string_patterns', defval, dscrpt)
    configLHCb.addOption('UserAddedApplications', "", "List of user added LHCb applications split by ':'")

    configLHCb.addOption('SplitByFilesBackend', 'OfflineGangaDiracSplitter',
                         'Possible SplitByFiles backend algorithms to use to split jobs into subjobs,\
                          options are: GangaDiracSplitter, OfflineGangaDiracSplitter, \
                          splitInputDataBySize and splitInputData')
    defaultLHCbDirac = 'prod'
    configLHCb.addOption('LHCbDiracVersion', defaultLHCbDirac, 'set LHCbDirac version')

    defaultPlatform = guessPlatform()
    configLHCb.addOption('defaultPlatform', defaultPlatform, 'The default platform for applications to use')

def _store_root_version():
    if 'ROOTSYS' in os.environ:
        vstart = os.environ['ROOTSYS'].find('ROOT/') + 5
        vend = os.environ['ROOTSYS'][vstart:].find('/')
        rootversion = os.environ['ROOTSYS'][vstart:vstart + vend]
        os.environ['ROOTVERSION'] = rootversion
    else:
        msg = 'Tried to setup ROOTVERSION environment variable but no ROOTSYS variable found.'
        raise OptionValueError(msg)


if not _after_bootstrap:
    store_dirac_environment()
    # _store_root_version()


def standardSetup():

    from . import PACKAGE
    PACKAGE.standardSetup()


def loadPlugins(config=None):
    logger.debug("Importing Backends")
    from .Lib import Backends
    logger.debug("Importing Applications")
    from .Lib import Applications
    logger.debug("Importing LHCbDataset")
    from .Lib import LHCbDataset
    logger.debug("Importing Mergers")
    from .Lib import Mergers
    logger.debug("Importing RTHandlers")
    from .Lib import RTHandlers
    logger.debug("Importing Splitters")
    from .Lib import Splitters
    logger.debug("Importing Tasks")
    from .Lib import Tasks
    logger.debug("Importing Files")
    from .Lib import Files
    logger.debug("Importing Checkers")
    from .Lib import Checkers
    logger.debug("Importing LHCbTasks")
    from .Lib import Tasks
    logger.debug("Finished Importing")


def postBootstrapHook():
    configDirac = GangaCore.Utility.Config.getConfig('DIRAC')
    configOutput = GangaCore.Utility.Config.getConfig('Output')
    configPoll = GangaCore.Utility.Config.getConfig('PollThread')
    configProxy = GangaCore.Utility.Config.getConfig('defaults_DiracProxy')

    configDirac.setSessionValue('DiracEnvJSON', os.environ['GANGADIRACENVIRONMENT'])
    configDirac.setSessionValue('userVO', 'lhcb')
    configDirac.setSessionValue('allDiracSE', ['CERN-USER', 'CNAF-USER', 'GRIDKA-USER', 'RRCKI-USER',
                                               'IN2P3-USER', 'SARA-USER', 'PIC-USER', 'RAL-USER'])
    configDirac.setSessionValue('noInputDataBannedSites', [])
    configDirac.setSessionValue('RequireDefaultSE', False)
    configDirac.setSessionValue('proxyInitCmd', 'lhcb-proxy-init')
    configDirac.setSessionValue('proxyInfoCmd', 'dirac-proxy-info')

    configOutput.setSessionValue('FailJobIfNoOutputMatched', 'False')

    configPoll.setSessionValue('autoCheckCredentials', False)

    configProxy.setSessionValue('group', 'lhcb_user')
    configProxy.setSessionValue('encodeDefaultProxyFileName', False)

# This is being dropped from 6.1.0 due to causing some bug in loading large numbers of jobs
#
# This will be nice to re-add once there is lazy loading support passed to the display for the 'jobs' command 09/2015 rcurrie
#
#from GangaCore.GPIDev.Lib.Registry.JobRegistry import config as display_config
#display_config.setSessionValue( 'jobs_columns', ('fqid', 'status', 'name', 'subjobs', 'application', 'backend', 'backend.actualCE', 'backend.extraInfo', 'comment') )
#display_config.setSessionValue( 'jobs_columns_functions', {'comment': 'lambda j: j.comment', 'backend.extraInfo': 'lambda j : j.backend.extraInfo ', 'subjobs': 'lambda j: len(j.subjobs)', 'backend.actualCE': 'lambda j:j.backend.actualCE', 'application': 'lambda j: j.application._name', 'backend': 'lambda j:j.backend._name'} )
#display_config.setSessionValue('jobs_columns_width', {'fqid': 8, 'status': 10, 'name': 10, 'application': 15, 'backend.extraInfo': 30, 'subjobs': 8, 'backend.actualCE': 17, 'comment': 20, 'backend': 15} )

    from GangaCore.Core.GangaThread.WorkerThreads import getQueues
    queue = getQueues()
    if queue is not None:
        queue.add(updateCreds)
    else:
        updateCreds()

def updateCreds():
    try:
        for group in ('lhcb_user', ):
            if group == 'lhcb_user':
                credential_store[DiracProxy(group=group, encodeDefaultProxyFileName=False)]
            credential_store[DiracProxy(group=group)]
    except KeyError:
        pass


class gridProxy(object):
    """
    This is a stub class which wraps functions from the `credential_store` sentinal to familiar functions from Ganga 6.2 and prior
    """

    @classmethod
    def renew(cls):
        """
        This method is similar to calling::

            credential_store.create(DiracProxy())

        or::

            credential_store[DiracProxy()].renew()

        as appropriate.
        """

        from GangaCore.GPI import credential_store, DiracProxy
        try:
            cred = credential_store[DiracProxy()]
            if not cred.is_valid():
                cred.create()
        except KeyError:
            credential_store.create(DiracProxy())

    @classmethod
    def create(cls):
        """
        This is a wrapper for::

            credential_store.create(DiracProxy())
        """
        cls.renew()

    @classmethod
    def destroy(cls):
        """
        This is a wrapper for::

            credential_store[DiracProxy()].destroy()
        """
        from GangaCore.GPI import credential_store, DiracProxy
        try:
            cred = credential_store[DiracProxy()]
            cred.destroy()
        except KeyError:
            pass

exportToGPI('gridProxy', gridProxy, 'Functions')
