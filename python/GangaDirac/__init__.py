import os
#from multiprocessing     import cpu_count
from Ganga.Utility.Config import makeConfig, getConfig
from Ganga.Utility.logging import getLogger

from Ganga.Utility.Config.Config import _after_bootstrap
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

    configDirac.addOption('ReplicateOutputData', False,
                      'Determines whether outputdata stored on Dirac is replicated')

    configDirac.addOption('allDiracSE', [], 'SE/Space-Tokens allowed for replication, writing files etc.')

    configDirac.addOption('DiracFileAutoGet', True, 'Should the DiracFile object automatically poll the Dirac backend for missing information on an lfn?')

    configDirac.addOption('OfflineSplitterFraction', 0.75, 'If subset is above OfflineSplitterFraction*filesPerJob then keep the subset')
    configDirac.addOption('OfflineSplitterMaxCommonSites', 3, 'Maximum number of storage sites all LFN should share in the same dataset. This is reduced to 1 as the splitter gets more desperate to group the data.')
    configDirac.addOption('OfflineSplitterUniqueSE', True, 'Should the Sites chosen be accessing different Storage Elements.')
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


def standardSetup():

    import PACKAGE
    PACKAGE.standardSetup()



def loadPlugins(config=None):
    logger.debug("Loading Backends")
    import Lib.Backends
    logger.debug("Loading RTHandlers")
    import Lib.RTHandlers
    logger.debug("Loading Files")
    import Lib.Files

