import os
import os.path
import re
import datetime
from os.path import exists, isdir, realpath, isfile, islink
from os import pathsep, listdir, environ, fdopen
import subprocess
import tempfile
import Ganga.Utility.logging
import Ganga.Utility.Config
from optparse import OptionParser, OptionValueError

from Ganga.Utility.Config.Config import _after_bootstrap
from Ganga.Utility.logging import getLogger
logger = getLogger()

if not _after_bootstrap:
    configLHCb = Ganga.Utility.Config.makeConfig('LHCb', 'Parameters for LHCb')
    configDirac = Ganga.Utility.Config.getConfig('DIRAC')

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
                      options are: GangaDiracSplitter, OfflineGangaDiracSplitter, splitInputDataBySize and splitInputData')

    configDirac.overrideDefaultValue('userVO', 'lhcb')
    configDirac.overrideDefaultValue('allDiracSE', ['CERN-USER', 'CNAF-USER', 'GRIDKA-USER', 'IN2P3-USER', 'SARA-USER', 'PIC-USER', 'RAL-USER'])
    configDirac.overrideDefaultValue('noInputDataBannedSites', ['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr', 'LCG.NIKHEF.nl', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.SARA.nl'])
    configDirac.overrideDefaultValue('RequireDefaultSE', False)


def _guess_version(name):
    if 'GANGASYSROOT' in os.environ.keys():
        gangasys = os.environ['GANGASYSROOT']
    else:
        raise OptionValueError("Can't guess %s version if GANGASYSROOT is not defined" % name)
    tmp = tempfile.NamedTemporaryFile(suffix='.txt')
    cmd = 'cd %s && cmt show projects > %s' % (gangasys, tmp.name)
    rc = subprocess.Popen([cmd], shell=True).wait()
    if rc != 0:
        msg = "Fail to get list of projects that Ganga depends on"
        raise OptionValueError(msg)
    p = re.compile(r'^\s*%s\s+%s_(\S+)\s+' % (name, name))
    for line in tmp:
        m = p.match(line)
        if m:
            version = m.group(1)
            return version
    msg = 'Failed to identify %s version that Ganga depends on' % name
    raise OptionValueError(msg)


def _store_root_version():
    if 'ROOTSYS' in os.environ:
        vstart = os.environ['ROOTSYS'].find('ROOT/') + 5
        vend = os.environ['ROOTSYS'][vstart:].find('/')
        rootversion = os.environ['ROOTSYS'][vstart:vstart + vend]
        os.environ['ROOTVERSION'] = rootversion
    else:
        msg = 'Tried to setup ROOTVERSION environment variable but no ROOTSYS variable found.'
        raise OptionValueError(msg)


def _store_dirac_environment():
    diracversion = _guess_version('LHCBDIRAC')
    import tempfile
    import subprocess
    import os
    platform = os.environ['CMTOPT']
    setup_script = 'SetupProject.sh'
    env = {}
    fdir = os.path.join(os.path.expanduser("~/.cache/LHCbDIRAC_ENV"), platform)
    if not os.path.exists(fdir):
        os.makedirs(fdir)
    fname = os.path.join(fdir, diracversion)
    if not os.path.exists(fname) or not os.path.getsize(fname):
        from Ganga.Utility.logging import getLogger
        log = getLogger()
        log.info("Storing new LHCbDirac environment (%s:%s)" % (str(diracversion), str(platform)))
        s_file = open(fname, 'w+')
        cmd = '/usr/bin/env bash -c \"source %s LHCBDIRAC %s ROOT>& /dev/null && '\
            'printenv > %s\"' % (setup_script, diracversion, fname)
        rc = subprocess.Popen([cmd], shell=True).wait()
        if rc != 0 or not os.path.exists(fname):
            msg = '--dirac: Failed to setup Dirac version %s as obtained from project dependency.' % diracversion
            raise OptionValueError(msg)
        count = 0
        for line in s_file.readlines():
            if line.find('DIRAC') >= 0:
                count += 1
            varval = line.strip().split('=')
            env[varval[0]] = ''.join(varval[1:])
        s_file.close()
        if count == 0:
            msg = 'Tried to setup Dirac version %s. For some reason this did not setup the DIRAC environment.' % diracversion
            raise OptionValueError(msg)
    os.environ['GANGADIRACENVIRONMENT'] = fname

if not _after_bootstrap:
    _store_dirac_environment()
    configDirac.setSessionValue('DiracEnvFile', os.environ['GANGADIRACENVIRONMENT'])

    _store_root_version()


def getEnvironment(config=None):
    import sys
    import os.path
    import PACKAGE

    PACKAGE.standardSetup()
    return {}


def loadPlugins(config=None):
    logger.debug("Importing Backends")
    import Lib.Backends
    logger.debug("Importing Applications")
    import Lib.Applications
    logger.debug("Importing LHCbDataset")
    import Lib.LHCbDataset
    logger.debug("Importing Mergers")
    import Lib.Mergers
    logger.debug("Importing RTHandlers")
    import Lib.RTHandlers
    logger.debug("Importing Splitters")
    import Lib.Splitters
    logger.debug("Importing Tasks")
    import Lib.Tasks
    logger.debug("Importing Files")
    import Lib.Files
    logger.debug("Importing Checkers")
    import Lib.Checkers
    logger.debug("Importing LHCbTasks")
    import Lib.Tasks
    logger.debug("Finished Importing")

from Ganga.GPIDev.Lib.File.Configure import outputconfig

outputconfig.overrideDefaultValue('FailJobIfNoOutputMatched', 'False')


# This is being dropped from 6.1.0 due to causing some bug in loading large numbers of jobs
#
# This will be nice to re-add once there is lazy loading support passed to the display for the 'jobs' command 09/2015 rcurrie
#
#from Ganga.GPIDev.Lib.Registry.JobRegistry import config as display_config
#display_config.overrideDefaultValue( 'jobs_columns', ('fqid', 'status', 'name', 'subjobs', 'application', 'backend', 'backend.actualCE', 'backend.extraInfo', 'comment') )
#display_config.overrideDefaultValue( 'jobs_columns_functions', {'comment': 'lambda j: j.comment', 'backend.extraInfo': 'lambda j : j.backend.extraInfo ', 'subjobs': 'lambda j: len(j.subjobs)', 'backend.actualCE': 'lambda j:j.backend.actualCE', 'application': 'lambda j: j.application._name', 'backend': 'lambda j:j.backend._name'} )
#display_config.overrideDefaultValue('jobs_columns_width', {'fqid': 8, 'status': 10, 'name': 10, 'application': 15, 'backend.extraInfo': 30, 'subjobs': 8, 'backend.actualCE': 17, 'comment': 20, 'backend': 15} )

