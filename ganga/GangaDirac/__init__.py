import os
#from multiprocessing     import cpu_count
from GangaCore.Utility.Config import makeConfig, getConfig
from GangaCore.Utility.logging import getLogger

from GangaCore.Utility.Config.Config import _after_bootstrap

from GangaCore.GPIDev.Credentials.CredentialStore import credential_store

logger = getLogger()

if not _after_bootstrap:
    configDirac = makeConfig('DIRAC', 'Parameters for DIRAC')
    config = getConfig('Configuration')

    # Set default values for the Dirac section.
    # configDirac.addOption('ShowDIRACstdout', False,
    #                      'Display DIRAC API stdout to the screen in Ganga?')

    configDirac.addOption('Timeout', 1000,
                      'Default timeout (seconds) for Dirac commands')

    configDirac.addOption('splitFilesChunks', 5000,
                      'when splitting datasets, pre split into chunks of this int')
    diracenv = ""
    if "GANGADIRACENVIRONMENT" in os.environ:
        diracenv = os.environ["GANGADIRACENVIRONMENT"]

    configDirac.addOption('DiracEnvFile', diracenv, 'DEPRECATED. Ganga environment file for DIRAC environment (do not change unless you are sure you know what you are doing).')

    configDirac.addOption('DiracEnvSource', None, 'File to be sourced to provide the DIRAC environment. E.g. /cvmfs/ganga.cern.ch/dirac_ui/bashrc')
    configDirac.addOption('DiracEnvJSON', None, 'A JSON file containing the environment for DIRAC. Overrides DiracEnvSource')

    configDirac.addOption('DiracCommandFiles', [os.path.join(os.path.dirname(__file__), 'Lib/Server/DiracDefinition.py'),
                                                os.path.join(os.path.dirname(__file__), 'Lib/Server/DiracCommands.py')],
                      'The file containing the python commands that the local DIRAC server can execute. The default DiracCommands.py is added automatically')

    configDirac.addOption('noInputDataBannedSites', [],
                      'List of sites to ban when a user job has no input data (this is meant to reduce the load on these sites)')

    configDirac.addOption('MaxDiracBulkJobs', 500, 'The Maximum allowed number of bulk submitted jobs before Ganga intervenes')

    configDirac.addOption('failed_sandbox_download', True, 'Automatically download sandbox for failed jobs?')

    configDirac.addOption('load_default_Dirac_backend', True, 'Whether or not to load the default dirac backend. This allows packages to load a modified version if necessary')

    ## TODO this would be nice to move from here once the credentials branch is up and running
    configDirac.addOption('userVO', '', 'The name of the VO that the user belongs to')

    configDirac.addOption('DiracLFNBase', '', "Base dir prepended to create LFN name from DiracFile('name'). If this is unset then it will default to /[userVO]/user/[first letter of user name]/[user name]")

    configDirac.addOption('useGangaPath', False, "Should we use the Ganga job ID to auto-construct a LFN relative path?")

    configDirac.addOption('ReplicateOutputData', False,
                      'Determines whether outputdata stored on Dirac is replicated')

    configDirac.addOption('allDiracSE', [], 'SE/Space-Tokens allowed for replication, writing files etc.')

    configDirac.addOption('DiracFileAutoGet', True, 'Should the DiracFile object automatically poll the Dirac backend for missing information on an lfn?')

    configDirac.addOption('OfflineSplitterFraction', 0.75, 'If subset is above OfflineSplitterFraction*filesPerJob then keep the subset')
    configDirac.addOption('OfflineSplitterMaxCommonSites', 2, 'Maximum number of storage sites all LFN should share in the same dataset. This is reduced to 1 as the splitter gets more desperate to group the data.')
    configDirac.addOption('OfflineSplitterUniqueSE', False, 'Should the Sites chosen be accessing different Storage Elements.')
    configDirac.addOption('OfflineSplitterLimit', 50,
                      'Number of iterations of selecting random Sites that are performed before the spliter reduces the OfflineSplitter fraction by raising it by 1 power and reduces OfflineSplitterMaxCommonSites by 1. Smaller number makes the splitter accept many smaller subsets higher means keeping more subsets but takes much more CPU to match files accordingly.')

    configDirac.addOption('RequireDefaultSE', True, 'Do we require the user to configure a defaultSE in some way?')

    configDirac.addOption('statusmapping', {'Checking': 'submitted',
                                            'Completed': 'running',
                                            'Deleted': 'failed',
                                            'Done': 'completed',
                                            'Failed': 'failed',
                                            'Killed': 'failed',
                                            'Matched': 'submitted',
                                            'Received': 'submitted',
                                            'Running': 'running',
                                            'Staging': 'submitted',
                                            'Stalled': 'running',
                                            'Waiting': 'submitted'}, "Mapping between Dirac Job Major Status and Ganga Job Status")

    configDirac.addOption('finalised_statuses',
                                                {'Done': 'completed',
                                                 'Failed': 'failed',
                                                 'Killed': 'failed',
                                                 'Deleted': 'failed',
                                                 'Unknown: No status for Job': 'failed'},
                                                "Mapping of Dirac to Ganga Job statuses used to construct a queue to finalize a given job, i.e. final statues in 'statusmapping'")

    configDirac.addOption('serializeBackend', False, 'Developer option to serialize Dirac code for profiling/debugging')

    configDirac.addOption('proxyInitCmd', 'dirac-proxy-init', 'Configurable which sets the default proxy init command for DIRAC')
    configDirac.addOption('proxyInfoCmd', 'dirac-proxy-info', 'Configurable which sets the default proxy init command for DIRAC')

    configDirac.addOption('maxSubjobsPerProcess', 100, 'Set the maximum number of subjobs to be submitted per process.')
    configDirac.addOption('maxSubjobsFinalisationPerProcess', 40, 'Set the maximum number of subjobs to be finalised per process. Not too high to avoid DIRAC timeouts')

    configDirac.addOption('default_finaliseOnMaster', False, 'Finalise all the subjobs in one go')
    configDirac.addOption('default_downloadOutputSandbox', True, 'Donwload output sandboxes by default')
    configDirac.addOption('default_unpackOutputSandbox', True, 'Unpack output sandboxes by default')

def standardSetup():

    from . import PACKAGE
    PACKAGE.standardSetup()



def loadPlugins(config=None):
    logger.debug("Loading Backends")
    from .Lib import Backends
    logger.debug("Loading RTHandlers")
    from .Lib import RTHandlers
    logger.debug("Loading Files")
    from .Lib import Files

def postBootstrapHook():
    from GangaCore.GPIDev.Lib.Config.Config import getConfig
    from GangaCore.Runtime.bootstrap import GangaProgram
    regenerate = False

    dirac_conf = getConfig('DIRAC')
    if not dirac_conf['DiracEnvSource'] and not dirac_conf['DiracEnvJSON']:
        logger.warning("The DIRAC UI bashrc file location is missing from your config [DIRAC]/DiracEnvSource section")
        source = input("Enter it now, [DIRAC]/DiracEnvSource: ")
        if not source:
            logger.fatal("No location specified, Dirac plugin will likely not work! Please fix your config file manually")
            raise ImportError("GangaDirac plugin incorrectly configured")
        dirac_conf.setGangarcValue("DiracEnvSource", source)
        regenerate = True

    dirac_proxy = getConfig("defaults_DiracProxy")
    if not dirac_proxy["group"] or dirac_proxy["group"] == "None":
        logger.warning("The DIRAC group for generating your proxy is missing from your config [defaults_DiracProxy]/group section")
        group = input("Enter it now (e.g. <VO>_user), [defaults_DiracProxy]/group: ")
        if not group:
            logger.fatal("No group specified, executing dirac commands will likely not work! Please fix your config file manually")
            raise ImportError("GangaDirac plugin incorrectly configured")
        dirac_proxy.setGangarcValue("group", group)
        regenerate=True
    if regenerate:
        GangaProgram.generate_config_file(getConfig("System")["GANGA_CONFIG_FILE"],
                                          interactive=False)

    from GangaDirac.Lib.Credentials.DiracProxy import DiracProxy
    try:
        credential_store[DiracProxy()]
    except KeyError:
        pass
